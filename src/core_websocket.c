#ifndef DISABLE_PEER_SIGNALING

#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "base64.h"
#include "core_websocket.h"
#include "mbedtls/sha1.h"
#include "ports.h"
#include "ssl_transport.h"
#include "utils.h"

#define WS_GUID "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
#define WS_HANDSHAKE_BUF_LEN 2048
#define WS_MAX_FRAME_HEADER_LEN 14

static int ws_url_parse(const char* url, int* secure, char* host, size_t host_len, uint16_t* port, char* path, size_t path_len) {
  const char* p;
  const char* host_start;
  const char* path_start;
  const char* port_start;
  size_t len;

  if (url == NULL || secure == NULL || host == NULL || port == NULL || path == NULL) {
    return -1;
  }

  if (strncmp(url, "wss://", 6) == 0) {
    *secure = 1;
    *port = 443;
    host_start = url + 6;
  } else if (strncmp(url, "ws://", 5) == 0) {
    *secure = 0;
    *port = 80;
    host_start = url + 5;
  } else {
    return -1;
  }

  path_start = strchr(host_start, '/');
  port_start = strchr(host_start, ':');

  if (path_start == NULL) {
    path_start = host_start + strlen(host_start);
  }

  if (port_start != NULL && port_start < path_start) {
    len = (size_t)(port_start - host_start);
    *port = (uint16_t)atoi(port_start + 1);
  } else {
    len = (size_t)(path_start - host_start);
  }

  if (len == 0 || len >= host_len) {
    return -1;
  }

  memcpy(host, host_start, len);
  host[len] = '\0';

  p = (*path_start == '/') ? path_start : "/";
  if (strlen(p) >= path_len) {
    return -1;
  }
  strcpy(path, p);

  return 0;
}

static int ws_transport_send(CoreWebSocketClient* ws, const void* buf, size_t len) {
  if (ws->secure) {
    return ssl_transport_send(ws->tls, buf, len);
  }
  return tcp_socket_send(&ws->tcp, (const uint8_t*)buf, (int)len);
}

static int ws_transport_recv(CoreWebSocketClient* ws, void* buf, size_t len) {
  if (ws->secure) {
    return ssl_transport_recv(ws->tls, buf, len);
  }
  return tcp_socket_recv(&ws->tcp, (uint8_t*)buf, (int)len);
}

static int ws_transport_connect(CoreWebSocketClient* ws) {
  Address addr;

  if (ws->secure) {
    ws->tls = (NetworkContext_t*)calloc(1, sizeof(NetworkContext_t));
    if (ws->tls == NULL) {
      return -1;
    }
    if (ssl_transport_connect(ws->tls, ws->host, ws->port, NULL) < 0) {
      free(ws->tls);
      ws->tls = NULL;
      return -1;
    }
    return 0;
  }

  memset(&addr, 0, sizeof(addr));
  if (tcp_socket_open(&ws->tcp, AF_INET) < 0) {
    return -1;
  }

  if (ports_resolve_addr(ws->host, &addr) < 0) {
    tcp_socket_close(&ws->tcp);
    return -1;
  }

  addr_set_port(&addr, ws->port);
  if (tcp_socket_connect(&ws->tcp, &addr) < 0) {
    tcp_socket_close(&ws->tcp);
    return -1;
  }

  return 0;
}

static void ws_transport_disconnect(CoreWebSocketClient* ws) {
  if (ws->secure) {
    if (ws->tls) {
      ssl_transport_disconnect(ws->tls);
      free(ws->tls);
      ws->tls = NULL;
    }
  } else {
    tcp_socket_close(&ws->tcp);
  }
}

static void ws_make_key(char* key, size_t key_len) {
  uint8_t raw[16];

  for (size_t i = 0; i < sizeof(raw); i += 4) {
    uint32_t r = ports_random_u32();
    raw[i] = (uint8_t)(r >> 24);
    raw[i + 1] = (uint8_t)(r >> 16);
    raw[i + 2] = (uint8_t)(r >> 8);
    raw[i + 3] = (uint8_t)r;
  }

  base64_encode(raw, sizeof(raw), key, (int)key_len);
}

static void ws_make_accept(const char* key, char* accept, size_t accept_len) {
  char input[128];
  unsigned char hash[20];

  snprintf(input, sizeof(input), "%s%s", key, WS_GUID);
  mbedtls_sha1((const unsigned char*)input, strlen(input), hash);
  base64_encode(hash, sizeof(hash), accept, (int)accept_len);
}

static int ws_strcasestr_contains(const char* haystack, const char* needle) {
  size_t nlen;

  if (haystack == NULL || needle == NULL) {
    return 0;
  }

  nlen = strlen(needle);
  if (nlen == 0) {
    return 1;
  }

  for (const char* p = haystack; *p; p++) {
    size_t i = 0;
    while (i < nlen && p[i] && tolower((unsigned char)p[i]) == tolower((unsigned char)needle[i])) {
      i++;
    }
    if (i == nlen) {
      return 1;
    }
  }

  return 0;
}

static int ws_read_http_response(CoreWebSocketClient* ws, char* buf, size_t len) {
  size_t used = 0;
  int idle = 0;

  while (used + 1 < len && idle < 30000) {
    int ret = ws_transport_recv(ws, buf + used, len - used - 1);
    if (ret < 0) {
      return -1;
    }
    if (ret == 0) {
      ports_sleep_ms(1);
      idle++;
      continue;
    }

    used += (size_t)ret;
    buf[used] = '\0';
    if (strstr(buf, "\r\n\r\n")) {
      return (int)used;
    }
  }

  return -1;
}

int core_websocket_connect(CoreWebSocketClient* ws, const char* url) {
  char key[32];
  char accept[64];
  char req[1024];
  char resp[WS_HANDSHAKE_BUF_LEN];
  int req_len;

  if (ws == NULL || url == NULL) {
    return -1;
  }

  memset(ws, 0, sizeof(*ws));
  if (ws_url_parse(url, &ws->secure, ws->host, sizeof(ws->host), &ws->port, ws->path, sizeof(ws->path)) < 0) {
    LOGE("Invalid WebSocket URL: %s", url);
    return -1;
  }

  if (ws_transport_connect(ws) < 0) {
    LOGE("WebSocket transport connect failed");
    return -1;
  }

  ws_make_key(key, sizeof(key));
  ws_make_accept(key, accept, sizeof(accept));

  req_len = snprintf(req, sizeof(req),
                     "GET %s HTTP/1.1\r\n"
                     "Host: %s:%u\r\n"
                     "Upgrade: websocket\r\n"
                     "Connection: Upgrade\r\n"
                     "Sec-WebSocket-Key: %s\r\n"
                     "Sec-WebSocket-Version: 13\r\n"
                     "\r\n",
                     ws->path, ws->host, ws->port, key);
  if (req_len <= 0 || req_len >= (int)sizeof(req)) {
    ws_transport_disconnect(ws);
    return -1;
  }

  if (ws_transport_send(ws, req, (size_t)req_len) != req_len) {
    ws_transport_disconnect(ws);
    return -1;
  }

  if (ws_read_http_response(ws, resp, sizeof(resp)) < 0) {
    LOGE("WebSocket handshake response timeout");
    ws_transport_disconnect(ws);
    return -1;
  }

  if (strstr(resp, " 101 ") == NULL ||
      !ws_strcasestr_contains(resp, "upgrade: websocket") ||
      !ws_strcasestr_contains(resp, accept)) {
    LOGE("WebSocket handshake rejected: %s", resp);
    ws_transport_disconnect(ws);
    return -1;
  }

  ws->connected = 1;
  LOGI("WebSocket connected: %s:%u%s", ws->host, ws->port, ws->path);
  return 0;
}

int core_websocket_send_text(CoreWebSocketClient* ws, const char* text) {
  uint8_t header[WS_MAX_FRAME_HEADER_LEN];
  uint8_t mask[4];
  size_t payload_len;
  size_t header_len = 0;
  uint8_t* frame;
  int ret;

  if (ws == NULL || !ws->connected || text == NULL) {
    return -1;
  }

  payload_len = strlen(text);
  header[header_len++] = 0x81;  // FIN + text

  if (payload_len < 126) {
    header[header_len++] = 0x80 | (uint8_t)payload_len;
  } else if (payload_len <= 0xFFFF) {
    header[header_len++] = 0x80 | 126;
    header[header_len++] = (uint8_t)(payload_len >> 8);
    header[header_len++] = (uint8_t)payload_len;
  } else {
    header[header_len++] = 0x80 | 127;
    for (int i = 7; i >= 0; i--) {
      header[header_len++] = (uint8_t)((uint64_t)payload_len >> (8 * i));
    }
  }

  for (size_t i = 0; i < sizeof(mask); i++) {
    mask[i] = (uint8_t)ports_random_u32();
    header[header_len++] = mask[i];
  }

  frame = (uint8_t*)malloc(header_len + payload_len);
  if (frame == NULL) {
    return -1;
  }

  memcpy(frame, header, header_len);
  for (size_t i = 0; i < payload_len; i++) {
    frame[header_len + i] = ((const uint8_t*)text)[i] ^ mask[i % 4];
  }

  ret = ws_transport_send(ws, frame, header_len + payload_len);
  free(frame);
  return ret == (int)(header_len + payload_len) ? 0 : -1;
}

static int ws_send_control(CoreWebSocketClient* ws, uint8_t opcode, const uint8_t* payload, size_t payload_len) {
  uint8_t header[6];
  uint8_t frame[6 + 125];
  uint8_t mask[4];

  if (payload_len > 125) {
    return -1;
  }

  header[0] = 0x80 | (opcode & 0x0F);
  header[1] = 0x80 | (uint8_t)payload_len;
  for (size_t i = 0; i < sizeof(mask); i++) {
    mask[i] = (uint8_t)ports_random_u32();
    header[2 + i] = mask[i];
  }

  memcpy(frame, header, sizeof(header));
  for (size_t i = 0; i < payload_len; i++) {
    frame[sizeof(header) + i] = payload[i] ^ mask[i % 4];
  }

  return ws_transport_send(ws, frame, sizeof(header) + payload_len) == (int)(sizeof(header) + payload_len) ? 0 : -1;
}

static int ws_read_exact(CoreWebSocketClient* ws, uint8_t* buf, size_t len) {
  size_t off = 0;
  int idle = 0;

  while (off < len && idle < 30000) {
    int ret = ws_transport_recv(ws, buf + off, len - off);
    if (ret < 0) {
      return -1;
    }
    if (ret == 0) {
      ports_sleep_ms(1);
      idle++;
      continue;
    }
    off += (size_t)ret;
  }

  return off == len ? 0 : -1;
}

int core_websocket_recv_text(CoreWebSocketClient* ws, char* buf, size_t len) {
  uint8_t hdr[2];
  uint8_t mask[4] = {0};
  uint64_t payload_len;
  int masked;
  int opcode;

  if (ws == NULL || !ws->connected || buf == NULL || len == 0) {
    return -1;
  }

  while (1) {
    if (ws_read_exact(ws, hdr, sizeof(hdr)) < 0) {
      return -1;
    }

    opcode = hdr[0] & 0x0F;
    masked = (hdr[1] & 0x80) != 0;
    payload_len = hdr[1] & 0x7F;

    if (payload_len == 126) {
      uint8_t ext[2];
      if (ws_read_exact(ws, ext, sizeof(ext)) < 0) {
        return -1;
      }
      payload_len = ((uint64_t)ext[0] << 8) | ext[1];
    } else if (payload_len == 127) {
      uint8_t ext[8];
      payload_len = 0;
      if (ws_read_exact(ws, ext, sizeof(ext)) < 0) {
        return -1;
      }
      for (int i = 0; i < 8; i++) {
        payload_len = (payload_len << 8) | ext[i];
      }
    }

    if (masked && ws_read_exact(ws, mask, sizeof(mask)) < 0) {
      return -1;
    }

    if (payload_len + 1 > len) {
      LOGE("WebSocket frame too large: %llu", (unsigned long long)payload_len);
      return -1;
    }

    if (ws_read_exact(ws, (uint8_t*)buf, (size_t)payload_len) < 0) {
      return -1;
    }

    if (masked) {
      for (uint64_t i = 0; i < payload_len; i++) {
        ((uint8_t*)buf)[i] ^= mask[i % 4];
      }
    }
    buf[payload_len] = '\0';

    if (opcode == 0x1) {
      return (int)payload_len;
    }
    if (opcode == 0x8) {
      ws->connected = 0;
      return 0;
    }
    if (opcode == 0x9) {
      ws_send_control(ws, 0xA, (const uint8_t*)buf, (size_t)payload_len);
      continue;
    }
    if (opcode == 0xA) {
      continue;
    }
  }
}

void core_websocket_close(CoreWebSocketClient* ws) {
  static const uint8_t close_frame[] = {0x88, 0x80, 0, 0, 0, 0};

  if (ws == NULL) {
    return;
  }

  if (ws->connected) {
    ws_transport_send(ws, close_frame, sizeof(close_frame));
  }
  ws_transport_disconnect(ws);
  ws->connected = 0;
}

#endif  // DISABLE_PEER_SIGNALING
