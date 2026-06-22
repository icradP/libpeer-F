/**
 * @file main.c
 * @brief ZLMediaKit P2P WebSocket signaling example.
 */

#include <cJSON.h>
#include <pthread.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>
#include <libgen.h>
#include <limits.h>

#include "core_websocket.h"
#include "peer.h"

#define ZLM_CLASS_KEY        "class"
#define ZLM_METHOD_KEY       "method"
#define ZLM_TRANSACTION_ID   "transaction_id"
#define ZLM_ROOM_ID          "room_id"
#define ZLM_FROM_ROOM_ID     "from_room_id"
#define ZLM_GUEST_ID         "guest_id"
#define ZLM_TYPE_KEY         "type"
#define ZLM_SDP_KEY          "sdp"
#define ZLM_VHOST_KEY        "vhost"
#define ZLM_APP_KEY          "app"
#define ZLM_STREAM_KEY       "stream"
#define ZLM_CANDIDATE_KEY    "candidate"
#define ZLM_UFRAG_KEY        "ufrag"
#define ZLM_PWD_KEY          "pwd"
#define ZLM_ICE_SERVERS_KEY  "ice_servers"

#define ZLM_CLASS_REQUEST    "request"
#define ZLM_CLASS_ACCEPT     "accept"
#define ZLM_CLASS_REJECT     "reject"
#define ZLM_CLASS_INDICATION "indication"

#define ZLM_METHOD_REGISTER  "register"
#define ZLM_METHOD_CALL      "call"
#define ZLM_METHOD_BYE       "bye"
#define ZLM_METHOD_CANDIDATE "candidate"

#define ZLM_TYPE_PLAY "play"
#define ZLM_TYPE_PUSH "push"

#define MAX_ICE_SERVERS 5
#define AUDIO_FRAME_BYTES 160
#define AUDIO_SEND_LOG_INTERVAL 100

static int g_interrupted = 0;
static int g_is_push = 0;
static int g_registered = 0;
static int g_call_done = 0;
static int g_signal_thread_started = 0;

static PeerConnection* g_pc = NULL;
static PeerConnectionState g_state = PEER_CONNECTION_CLOSED;
static CoreWebSocketClient g_ws;

static char g_ws_url[512] = {0};
static char g_signaling_host[256] = {0};
static int g_signaling_port = 3000;
static char g_app[64] = "live";
static char g_stream[128] = "p2p-stream";
static char g_my_room_id[128] = {0};
static char g_peer_room_id[128] = {0};
static char g_guest_id[128] = {0};
static char g_peer_guest_id[128] = {0};
static char g_last_call_transaction_id[64] = {0};

static char g_ice_urls[MAX_ICE_SERVERS][256];
static char g_ice_usernames[MAX_ICE_SERVERS][128];
static char g_ice_credentials[MAX_ICE_SERVERS][128];
static int g_ice_server_count = 0;

static char g_audio_file[512] = {0};
static int g_audio_file_explicit = 0;

static void signal_handler(int sig) {
  (void)sig;
  g_interrupted = 1;
}

static void copy_json_string(cJSON* root, const char* key, char* dst, size_t dst_len) {
  cJSON* item = cJSON_GetObjectItem(root, key);
  if (cJSON_IsString(item) && item->valuestring && dst_len > 0) {
    strncpy(dst, item->valuestring, dst_len - 1);
    dst[dst_len - 1] = '\0';
  }
}

static const char* json_string(cJSON* root, const char* key) {
  cJSON* item = cJSON_GetObjectItem(root, key);
  return cJSON_IsString(item) ? item->valuestring : NULL;
}

static void generate_id(char* buf, size_t len, const char* prefix) {
  static const char chars[] = "abcdefghijklmnopqrstuvwxyz0123456789";
  size_t off;

  if (len == 0) {
    return;
  }

  snprintf(buf, len, "%s", prefix);
  off = strlen(buf);
  for (size_t i = off; i + 1 < len; i++) {
    buf[i] = chars[rand() % (sizeof(chars) - 1)];
  }
  buf[len - 1] = '\0';
}

static char* json_print_and_delete(cJSON* root) {
  char* text = cJSON_PrintUnformatted(root);
  cJSON_Delete(root);
  return text;
}

static char* build_register_request(void) {
  char tx[64];
  cJSON* root = cJSON_CreateObject();

  generate_id(tx, sizeof(tx), "tx-");
  cJSON_AddStringToObject(root, ZLM_CLASS_KEY, ZLM_CLASS_REQUEST);
  cJSON_AddStringToObject(root, ZLM_METHOD_KEY, ZLM_METHOD_REGISTER);
  cJSON_AddStringToObject(root, ZLM_TRANSACTION_ID, tx);
  cJSON_AddStringToObject(root, ZLM_ROOM_ID, g_my_room_id);
  return json_print_and_delete(root);
}

static char* build_call_request(const char* sdp) {
  char tx[64];
  cJSON* root = cJSON_CreateObject();

  generate_id(tx, sizeof(tx), "tx-");
  cJSON_AddStringToObject(root, ZLM_CLASS_KEY, ZLM_CLASS_REQUEST);
  cJSON_AddStringToObject(root, ZLM_METHOD_KEY, ZLM_METHOD_CALL);
  cJSON_AddStringToObject(root, ZLM_TRANSACTION_ID, tx);
  cJSON_AddStringToObject(root, ZLM_GUEST_ID, g_guest_id);
  cJSON_AddStringToObject(root, ZLM_ROOM_ID, g_peer_room_id);
  cJSON_AddStringToObject(root, ZLM_FROM_ROOM_ID, g_my_room_id);
  cJSON_AddStringToObject(root, ZLM_VHOST_KEY, "__defaultVhost__");
  cJSON_AddStringToObject(root, ZLM_APP_KEY, g_app);
  cJSON_AddStringToObject(root, ZLM_STREAM_KEY, g_stream);
  cJSON_AddStringToObject(root, ZLM_TYPE_KEY, g_is_push ? ZLM_TYPE_PUSH : ZLM_TYPE_PLAY);
  cJSON_AddStringToObject(root, ZLM_SDP_KEY, sdp);
  return json_print_and_delete(root);
}

static char* build_accept_response(const char* sdp) {
  cJSON* root = cJSON_CreateObject();

  cJSON_AddStringToObject(root, ZLM_CLASS_KEY, ZLM_CLASS_ACCEPT);
  cJSON_AddStringToObject(root, ZLM_METHOD_KEY, ZLM_METHOD_CALL);
  cJSON_AddStringToObject(root, ZLM_TRANSACTION_ID, g_last_call_transaction_id);
  cJSON_AddStringToObject(root, ZLM_GUEST_ID, g_peer_guest_id);
  cJSON_AddStringToObject(root, ZLM_ROOM_ID, g_my_room_id);
  cJSON_AddStringToObject(root, ZLM_FROM_ROOM_ID, g_my_room_id);
  cJSON_AddStringToObject(root, ZLM_VHOST_KEY, "__defaultVhost__");
  cJSON_AddStringToObject(root, ZLM_APP_KEY, g_app);
  cJSON_AddStringToObject(root, ZLM_STREAM_KEY, g_stream);
  cJSON_AddStringToObject(root, ZLM_TYPE_KEY, g_is_push ? ZLM_TYPE_PUSH : ZLM_TYPE_PLAY);
  cJSON_AddStringToObject(root, ZLM_SDP_KEY, sdp);
  return json_print_and_delete(root);
}

static char* build_bye_message(void) {
  char tx[64];
  cJSON* root = cJSON_CreateObject();

  generate_id(tx, sizeof(tx), "tx-");
  cJSON_AddStringToObject(root, ZLM_CLASS_KEY, ZLM_CLASS_INDICATION);
  cJSON_AddStringToObject(root, ZLM_METHOD_KEY, ZLM_METHOD_BYE);
  cJSON_AddStringToObject(root, ZLM_TRANSACTION_ID, tx);
  cJSON_AddStringToObject(root, ZLM_GUEST_ID, g_guest_id);
  cJSON_AddStringToObject(root, ZLM_ROOM_ID, g_peer_room_id);
  cJSON_AddStringToObject(root, ZLM_FROM_ROOM_ID, g_my_room_id);
  return json_print_and_delete(root);
}

static int ws_send_json(char* msg) {
  int ret;

  if (!msg) {
    return -1;
  }
  printf("[信令] 发送: %s\n", msg);
  ret = core_websocket_send_text(&g_ws, msg);
  free(msg);
  return ret;
}

static void parse_ice_servers(cJSON* root) {
  cJSON* servers = cJSON_GetObjectItem(root, ZLM_ICE_SERVERS_KEY);
  int count = 0;

  if (!cJSON_IsArray(servers)) {
    servers = cJSON_GetObjectItem(root, "iceServers");
  }
  if (!cJSON_IsArray(servers)) {
    return;
  }

  cJSON* server = NULL;
  cJSON_ArrayForEach(server, servers) {
    cJSON* urls = cJSON_GetObjectItem(server, "urls");
    cJSON* username = cJSON_GetObjectItem(server, "username");
    cJSON* credential = cJSON_GetObjectItem(server, "credential");

    if (!urls) {
      urls = cJSON_GetObjectItem(server, "url");
    }
    if (!urls) {
      urls = cJSON_GetObjectItem(server, "uri");
    }
    if (!username) {
      username = cJSON_GetObjectItem(server, "ufrag");
    }
    if (!credential) {
      credential = cJSON_GetObjectItem(server, "pwd");
    }
    if (!credential) {
      credential = cJSON_GetObjectItem(server, "password");
    }

    if (count >= MAX_ICE_SERVERS) {
      break;
    }

    if (cJSON_IsArray(urls)) {
      urls = cJSON_GetArrayItem(urls, 0);
    }
    if (!cJSON_IsString(urls) || !urls->valuestring) {
      continue;
    }

    strncpy(g_ice_urls[count], urls->valuestring, sizeof(g_ice_urls[count]) - 1);
    if (cJSON_IsString(username) && username->valuestring) {
      strncpy(g_ice_usernames[count], username->valuestring, sizeof(g_ice_usernames[count]) - 1);
    }
    if (cJSON_IsString(credential) && credential->valuestring) {
      strncpy(g_ice_credentials[count], credential->valuestring, sizeof(g_ice_credentials[count]) - 1);
    }
    printf("[信令] ICE server[%d]: %s\n", count, g_ice_urls[count]);
    count++;
  }

  g_ice_server_count = count;
}

static char* build_candidate_message(const char* candidate, const char* sdp_mid) {
  char tx[64];
  cJSON* root = cJSON_CreateObject();

  generate_id(tx, sizeof(tx), "tx-");
  cJSON_AddStringToObject(root, ZLM_CLASS_KEY, ZLM_CLASS_INDICATION);
  cJSON_AddStringToObject(root, ZLM_METHOD_KEY, ZLM_METHOD_CANDIDATE);
  cJSON_AddStringToObject(root, ZLM_TRANSACTION_ID, tx);
  cJSON_AddStringToObject(root, ZLM_GUEST_ID, g_guest_id);
  cJSON_AddStringToObject(root, ZLM_ROOM_ID, g_peer_room_id);
  cJSON_AddStringToObject(root, ZLM_FROM_ROOM_ID, g_my_room_id);
  cJSON_AddStringToObject(root, ZLM_CANDIDATE_KEY, candidate);
  cJSON_AddStringToObject(root, "sdpMid", sdp_mid ? sdp_mid : "0");
  cJSON_AddNumberToObject(root, "sdpMLineIndex", 0);
  return json_print_and_delete(root);
}

static void send_candidates_from_sdp(const char* sdp, const char* sdp_mid) {
  const char* p;

  if (!sdp || !g_peer_room_id[0]) {
    return;
  }

  p = sdp;
  while ((p = strstr(p, "a=candidate:")) != NULL) {
    const char* end = strstr(p, "\r\n");
    char line[512];
    const char* cand;
    size_t len;

    if (!end) {
      break;
    }

    len = (size_t)(end - p);
    if (len >= sizeof(line)) {
      len = sizeof(line) - 1;
    }
    memcpy(line, p, len);
    line[len] = '\0';

    cand = line;
    if (strncmp(cand, "a=", 2) == 0) {
      cand += 2;
    }
    ws_send_json(build_candidate_message(cand, sdp_mid));
    p = end;
  }
}

static int handle_register_response(cJSON* root) {
  const char* cls = json_string(root, ZLM_CLASS_KEY);
  const char* method = json_string(root, ZLM_METHOD_KEY);

  if (!cls || !method || strcmp(method, ZLM_METHOD_REGISTER) != 0) {
    return 0;
  }

  if (strcmp(cls, ZLM_CLASS_REJECT) == 0) {
    printf("[信令] register rejected: %s\n", cJSON_PrintUnformatted(root));
    return -1;
  }

  if (strcmp(cls, ZLM_CLASS_ACCEPT) != 0) {
    return 0;
  }

  copy_json_string(root, ZLM_ROOM_ID, g_my_room_id, sizeof(g_my_room_id));
  parse_ice_servers(root);
  g_registered = 1;
  printf("[信令] 注册成功 room_id=%s guest_id=%s\n", g_my_room_id, g_guest_id);
  return 1;
}

static int wait_register_response(void) {
  char buf[16384];
  int ret;

  while (!g_interrupted) {
    ret = core_websocket_recv_text(&g_ws, buf, sizeof(buf));
    if (ret <= 0) {
      return -1;
    }

    printf("[信令] 收到: %s\n", buf);
    cJSON* root = cJSON_Parse(buf);
    if (!root) {
      continue;
    }
    ret = handle_register_response(root);
    cJSON_Delete(root);
    if (ret != 0) {
      return ret > 0 ? 0 : -1;
    }
  }

  return -1;
}

static void fill_test_pcma_frame(uint8_t* buf, size_t len) {
  /* Fallback when no A-law file: audible buzz (not G.711 silence 0xD5). */
  for (size_t i = 0; i < len; i++) {
    buf[i] = (i & 16) ? (uint8_t)0x90 : (uint8_t)0xD0;
  }
}

static int resolve_audio_near_executable(const char* argv0, char* dst, size_t dst_len) {
  char path[PATH_MAX];
  char exe_copy[PATH_MAX];
  const char* dir;
  const char* rel_paths[] = {
      "/ch_eagles.alaw",
      "/examples/assets/ch_eagles.alaw",
      "/../../../ch_eagles.alaw",
      "/../../../examples/assets/ch_eagles.alaw",
      NULL,
  };

  if (!argv0 || !argv0[0]) {
    return -1;
  }

  strncpy(exe_copy, argv0, sizeof(exe_copy) - 1);
  exe_copy[sizeof(exe_copy) - 1] = '\0';
  dir = dirname(exe_copy);
  if (!dir) {
    return -1;
  }

  for (int i = 0; rel_paths[i] != NULL; i++) {
    snprintf(path, sizeof(path), "%s%s", dir, rel_paths[i]);
    FILE* fp = fopen(path, "rb");
    if (fp) {
      fclose(fp);
      strncpy(dst, path, dst_len - 1);
      dst[dst_len - 1] = '\0';
      return 0;
    }
  }
  return -1;
}

static int resolve_default_audio_file(const char* argv0, char* dst, size_t dst_len) {
  static const char* candidates[] = {
      "ch_eagles.alaw",
      "examples/assets/ch_eagles.alaw",
      "../ch_eagles.alaw",
      "../../../ch_eagles.alaw",
      "../../../examples/assets/ch_eagles.alaw",
      NULL,
  };

  for (int i = 0; candidates[i] != NULL; i++) {
    FILE* fp = fopen(candidates[i], "rb");
    if (fp) {
      fclose(fp);
      strncpy(dst, candidates[i], dst_len - 1);
      dst[dst_len - 1] = '\0';
      return 0;
    }
  }

  return resolve_audio_near_executable(argv0, dst, dst_len);
}

static int read_push_audio_frame(FILE* fp, uint8_t* buf, size_t len) {
  size_t n;

  if (fp) {
    n = fread(buf, 1, len, fp);
    if (n == 0) {
      fseek(fp, 0, SEEK_SET);
      n = fread(buf, 1, len, fp);
    }
    if (n == 0) {
      return -1;
    }
    if (n < len) {
      memset(buf + n, 0xD5, len - n);
    }
    return (int)len;
  }

  fill_test_pcma_frame(buf, len);
  return (int)len;
}

static void on_connection_state(PeerConnectionState state, void* data) {
  (void)data;
  printf("[P2P] 连接状态: %s\n", peer_connection_state_to_string(state));
  g_state = state;
  if (state == PEER_CONNECTION_COMPLETED && g_is_push) {
    printf("[P2P] DTLS 完成，开始推送 PCMA 音频 (每 %d 帧打印一次)\n", AUDIO_SEND_LOG_INTERVAL);
  }
}

static void on_ice_candidate(char* sdp, void* userdata) {
  (void)userdata;
  printf("[P2P] ICE gathering completed, local SDP size=%zu\n", sdp ? strlen(sdp) : 0);
  send_candidates_from_sdp(sdp, "0");
}

static void on_audio_track(uint8_t* data, size_t size, void* userdata) {
  static uint32_t packets = 0;
  (void)data;
  (void)userdata;
  packets++;
  if (packets % 50 == 0) {
    printf("[P2P] 收到音频包: size=%zu packets=%u\n", size, packets);
  }
}

static int send_call_offer(void) {
  const char* offer = peer_connection_create_offer(g_pc);
  if (!offer) {
    return -1;
  }
  return ws_send_json(build_call_request(offer));
}

static int handle_call_request(cJSON* root) {
  const char* sdp = json_string(root, ZLM_SDP_KEY);
  const char* answer;

  if (!sdp) {
    printf("[信令] call request without SDP\n");
    return -1;
  }

  copy_json_string(root, ZLM_TRANSACTION_ID, g_last_call_transaction_id, sizeof(g_last_call_transaction_id));
  copy_json_string(root, ZLM_GUEST_ID, g_peer_guest_id, sizeof(g_peer_guest_id));
  copy_json_string(root, ZLM_FROM_ROOM_ID, g_peer_room_id, sizeof(g_peer_room_id));

  printf("[信令] 收到 call request, peer_room=%s peer_guest=%s\n", g_peer_room_id, g_peer_guest_id);
  peer_connection_set_remote_description(g_pc, sdp, SDP_TYPE_OFFER);
  answer = peer_connection_create_answer(g_pc);
  if (!answer) {
    return -1;
  }

  return ws_send_json(build_accept_response(answer));
}

static int handle_call_accept(cJSON* root) {
  const char* sdp = json_string(root, ZLM_SDP_KEY);

  if (!sdp) {
    printf("[信令] call accept without SDP\n");
    return -1;
  }

  copy_json_string(root, ZLM_GUEST_ID, g_peer_guest_id, sizeof(g_peer_guest_id));
  printf("[信令] call accepted, peer_guest=%s\n", g_peer_guest_id);
  peer_connection_set_remote_description(g_pc, sdp, SDP_TYPE_ANSWER);
  g_call_done = 1;
  return 0;
}

static int handle_candidate(cJSON* root) {
  const char* candidate = json_string(root, ZLM_CANDIDATE_KEY);
  if (!candidate) {
    return 0;
  }
  printf("[信令] 收到 candidate: %.80s\n", candidate);
  return peer_connection_add_ice_candidate(g_pc, (char*)candidate);
}

static void handle_signaling_message(const char* msg) {
  cJSON* root = cJSON_Parse(msg);
  const char* cls;
  const char* method;

  if (!root) {
    printf("[信令] JSON parse failed: %s\n", msg);
    return;
  }

  cls = json_string(root, ZLM_CLASS_KEY);
  method = json_string(root, ZLM_METHOD_KEY);

  if (!cls || !method) {
    cJSON_Delete(root);
    return;
  }

  if (strcmp(cls, ZLM_CLASS_REQUEST) == 0 && strcmp(method, ZLM_METHOD_CALL) == 0) {
    handle_call_request(root);
  } else if (strcmp(cls, ZLM_CLASS_ACCEPT) == 0 && strcmp(method, ZLM_METHOD_CALL) == 0) {
    handle_call_accept(root);
  } else if (strcmp(cls, ZLM_CLASS_REJECT) == 0) {
    printf("[信令] rejected: %s/%s\n", cls, method);
  } else if (strcmp(cls, ZLM_CLASS_INDICATION) == 0 && strcmp(method, ZLM_METHOD_CANDIDATE) == 0) {
    handle_candidate(root);
  } else if (strcmp(cls, ZLM_CLASS_INDICATION) == 0 && strcmp(method, ZLM_METHOD_BYE) == 0) {
    printf("[信令] 收到 bye\n");
    g_interrupted = 1;
  } else {
    printf("[信令] 未处理消息: class=%s method=%s\n", cls, method);
  }

  cJSON_Delete(root);
}

static void* signaling_task(void* data) {
  char buf[16384];
  (void)data;

  while (!g_interrupted) {
    int ret = core_websocket_recv_text(&g_ws, buf, sizeof(buf));
    if (ret < 0) {
      printf("[信令] WebSocket recv failed\n");
      g_interrupted = 1;
      break;
    }
    if (ret == 0) {
      printf("[信令] WebSocket closed\n");
      g_interrupted = 1;
      break;
    }
    printf("[信令] 收到: %s\n", buf);
    handle_signaling_message(buf);
  }

  return NULL;
}

static void* peer_connection_task(void* data) {
  while (!g_interrupted) {
    peer_connection_loop((PeerConnection*)data);
    usleep(1000);
  }
  return NULL;
}

static void print_usage(const char* prog) {
  printf("用法: %s -u <ws://host:port/path|wss://host:port/path> --my-room <id> [--peer-room <id>] [--push|--play]\n", prog);
  printf("\n参数:\n");
  printf("  -u <url>          ZLM P2P WebSocket URL\n");
  printf("  -H <host>         兼容旧参数: 信令服务器地址，未指定 -u 时组成 ws://host:port/\n");
  printf("  -p <port>         兼容旧参数: 信令服务器端口 (默认 3000)\n");
  printf("  -a <app>          应用名 (默认 live)\n");
  printf("  -s <stream>       流名 (默认 p2p-stream)\n");
  printf("  -A <file>         推流音频: 原始 G.711 A-law (PCMA 8kHz mono, 20ms=160字节)\n");
  printf("  --audio-file <f>  同 -A；push 未指定时自动尝试 ch_eagles.alaw\n");
  printf("  --my-room <id>    本端 room_id\n");
  printf("  --peer-room <id>  对端 room_id，play 模式必填，push 可从 call 中学习\n");
  printf("  --push            推流端模式: 等待 call 并发送 PCMA 音频\n");
  printf("  --play            拉流端模式: 主动 request/call\n");
}

static int parse_args(int argc, char* argv[]) {
  for (int i = 1; i < argc; i++) {
    if (strcmp(argv[i], "-u") == 0 && i + 1 < argc) {
      strncpy(g_ws_url, argv[++i], sizeof(g_ws_url) - 1);
    } else if (strcmp(argv[i], "-H") == 0 && i + 1 < argc) {
      strncpy(g_signaling_host, argv[++i], sizeof(g_signaling_host) - 1);
    } else if (strcmp(argv[i], "-p") == 0 && i + 1 < argc) {
      g_signaling_port = atoi(argv[++i]);
    } else if (strcmp(argv[i], "-a") == 0 && i + 1 < argc) {
      strncpy(g_app, argv[++i], sizeof(g_app) - 1);
    } else if (strcmp(argv[i], "-s") == 0 && i + 1 < argc) {
      strncpy(g_stream, argv[++i], sizeof(g_stream) - 1);
    } else if ((strcmp(argv[i], "-A") == 0 || strcmp(argv[i], "--audio-file") == 0) && i + 1 < argc) {
      strncpy(g_audio_file, argv[++i], sizeof(g_audio_file) - 1);
      g_audio_file_explicit = 1;
    } else if (strcmp(argv[i], "--my-room") == 0 && i + 1 < argc) {
      strncpy(g_my_room_id, argv[++i], sizeof(g_my_room_id) - 1);
    } else if (strcmp(argv[i], "--peer-room") == 0 && i + 1 < argc) {
      strncpy(g_peer_room_id, argv[++i], sizeof(g_peer_room_id) - 1);
    } else if (strcmp(argv[i], "--push") == 0) {
      g_is_push = 1;
    } else if (strcmp(argv[i], "--play") == 0) {
      g_is_push = 0;
    } else {
      return -1;
    }
  }

  if (!g_ws_url[0] && g_signaling_host[0]) {
    snprintf(g_ws_url, sizeof(g_ws_url), "ws://%s:%d/", g_signaling_host, g_signaling_port);
  }

  if (!g_ws_url[0] || !g_my_room_id[0] || (!g_is_push && !g_peer_room_id[0])) {
    return -1;
  }

  return 0;
}

static void fill_peer_config(PeerConfiguration* config) {
  memset(config, 0, sizeof(*config));

  if (g_ice_server_count == 0) {
    g_ice_server_count = 1;
    strncpy(g_ice_urls[0], "stun:www.icrad.ltd:3478", sizeof(g_ice_urls[0]) - 1);
  }

  for (int i = 0; i < g_ice_server_count && i < MAX_ICE_SERVERS; i++) {
    config->ice_servers[i].urls = g_ice_urls[i];
    config->ice_servers[i].username = g_ice_usernames[i][0] ? g_ice_usernames[i] : NULL;
    config->ice_servers[i].credential = g_ice_credentials[i][0] ? g_ice_credentials[i] : NULL;
  }

  config->datachannel = DATA_CHANNEL_NONE;
  config->video_codec = CODEC_NONE;
  config->audio_codec = CODEC_PCMA;
  /* Browser/ZLM P2P uses numeric mids (0/1) and trickle ICE like WHIP/WHEP,
   * not libpeer P2P mids (audio/video). */
  config->sdp_profile = g_is_push ? SDP_PROFILE_WHIP : SDP_PROFILE_WHEP;
  config->onaudiotrack = on_audio_track;
}

int main(int argc, char* argv[]) {
  pthread_t pc_thread;
  pthread_t signal_thread;
  uint64_t audio_time = 0;
  uint32_t audio_packets_sent = 0;
  uint32_t audio_send_failures = 0;
  FILE* fp_audio = NULL;
  const char* audio_source_label = "内置测试音 (PCMA)";

  if (parse_args(argc, argv) < 0) {
    print_usage(argv[0]);
    return 1;
  }

  if (g_is_push) {
    if (g_audio_file_explicit) {
      fp_audio = fopen(g_audio_file, "rb");
      if (!fp_audio) {
        printf("[错误] 无法打开音频文件: %s\n", g_audio_file);
        return 1;
      }
      audio_source_label = g_audio_file;
    } else if (resolve_default_audio_file(argv[0], g_audio_file, sizeof(g_audio_file)) == 0) {
      fp_audio = fopen(g_audio_file, "rb");
      if (fp_audio) {
        audio_source_label = g_audio_file;
      }
    }
  }

  signal(SIGINT, signal_handler);
  srand((unsigned int)time(NULL));
  generate_id(g_guest_id, sizeof(g_guest_id), "guest-");

  printf("========================================\n");
  printf(" ZLMediaKit P2P WebSocket 示例\n");
  printf("========================================\n");
  printf(" WebSocket: %s\n", g_ws_url);
  printf(" App/Stream: %s/%s\n", g_app, g_stream);
  printf(" My room: %s\n", g_my_room_id);
  printf(" Peer room: %s\n", g_peer_room_id[0] ? g_peer_room_id : "(from incoming call)");
  printf(" Guest: %s\n", g_guest_id);
  printf(" Mode: %s\n", g_is_push ? "push" : "play");
  printf(" SDP: %s\n", g_is_push ? "WHIP (sendonly, mid:0)" : "WHEP (recvonly, mid:0)");
  if (g_is_push) {
    printf(" Audio: PCMA/8000 — %s\n", audio_source_label);
  }
  printf("========================================\n\n");

  peer_init();

  if (core_websocket_connect(&g_ws, g_ws_url) < 0) {
    printf("[错误] WebSocket 连接失败\n");
    peer_deinit();
    return 1;
  }

  if (ws_send_json(build_register_request()) < 0 || wait_register_response() < 0 || !g_registered) {
    printf("[错误] register 失败\n");
    core_websocket_close(&g_ws);
    peer_deinit();
    return 1;
  }

  PeerConfiguration config;
  fill_peer_config(&config);
  g_pc = peer_connection_create(&config);
  if (!g_pc) {
    printf("[错误] 创建 PeerConnection 失败\n");
    core_websocket_close(&g_ws);
    peer_deinit();
    return 1;
  }

  peer_connection_oniceconnectionstatechange(g_pc, on_connection_state);
  peer_connection_onicecandidate(g_pc, on_ice_candidate);

  pthread_create(&pc_thread, NULL, peer_connection_task, g_pc);
  pthread_create(&signal_thread, NULL, signaling_task, NULL);
  g_signal_thread_started = 1;

  if (!g_is_push && send_call_offer() < 0) {
    printf("[错误] 发送 call offer 失败\n");
    g_interrupted = 1;
  }

  while (!g_interrupted) {
    PeerConnectionState state = peer_connection_get_state(g_pc);

    if (state == PEER_CONNECTION_COMPLETED && g_is_push) {
      struct timeval tv;
      gettimeofday(&tv, NULL);
      uint64_t now = (uint64_t)tv.tv_sec * 1000 + tv.tv_usec / 1000;

      if (now - audio_time >= 20) {
        uint8_t audio_buf[AUDIO_FRAME_BYTES];
        int frame_len;
        int send_ret;

        audio_time = now;
        frame_len = read_push_audio_frame(fp_audio, audio_buf, sizeof(audio_buf));
        if (frame_len <= 0) {
          printf("[P2P] 读取音频帧失败\n");
          continue;
        }

        send_ret = peer_connection_send_audio(g_pc, audio_buf, (size_t)frame_len);
        if (send_ret == 0) {
          audio_packets_sent++;
          if (audio_packets_sent == 1 || audio_packets_sent % AUDIO_SEND_LOG_INTERVAL == 0) {
            printf("[P2P] 已发送音频包: count=%u bytes=%d src=%s\n",
                   audio_packets_sent, frame_len, audio_source_label);
          }
        } else {
          audio_send_failures++;
          if (audio_send_failures == 1 || audio_send_failures % AUDIO_SEND_LOG_INTERVAL == 0) {
            printf("[P2P] 发送音频失败: ret=%d state=%s failures=%u\n",
                   send_ret, peer_connection_state_to_string(peer_connection_get_state(g_pc)),
                   audio_send_failures);
          }
        }
      }
    } else if (g_is_push && state != PEER_CONNECTION_COMPLETED && audio_packets_sent == 0) {
      static uint64_t last_wait_log = 0;
      struct timeval tv;
      gettimeofday(&tv, NULL);
      uint64_t now = (uint64_t)tv.tv_sec * 1000 + tv.tv_usec / 1000;
      if (last_wait_log == 0 || now - last_wait_log >= 5000) {
        printf("[P2P] 等待 DTLS completed (ICE=%s，需先完成 DTLS 才能发音频)\n",
               peer_connection_state_to_string(state));
        last_wait_log = now;
      }
    }
    usleep(1000);
  }

  printf("\n[P2P] 正在清理...\n");

  if (fp_audio) {
    fclose(fp_audio);
  }

  if (g_peer_room_id[0]) {
    ws_send_json(build_bye_message());
  }

  core_websocket_close(&g_ws);

  if (g_signal_thread_started) {
    pthread_join(signal_thread, NULL);
  }
  pthread_join(pc_thread, NULL);

  peer_connection_destroy(g_pc);
  peer_deinit();

  printf("[P2P] 已退出 call_done=%d\n", g_call_done);
  return 0;
}
