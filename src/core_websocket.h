#ifndef CORE_WEBSOCKET_H_
#define CORE_WEBSOCKET_H_

#ifndef DISABLE_PEER_SIGNALING

#include <stddef.h>
#include <stdint.h>

#include "socket.h"

#ifdef __cplusplus
extern "C" {
#endif

#define CORE_WEBSOCKET_HOST_MAX_LEN 128
#define CORE_WEBSOCKET_PATH_MAX_LEN 256

typedef struct CoreWebSocketClient {
  int secure;
  int connected;
  char host[CORE_WEBSOCKET_HOST_MAX_LEN];
  char path[CORE_WEBSOCKET_PATH_MAX_LEN];
  uint16_t port;
  struct NetworkContext* tls;
  TcpSocket tcp;
} CoreWebSocketClient;

int core_websocket_connect(CoreWebSocketClient* ws, const char* url);

int core_websocket_send_text(CoreWebSocketClient* ws, const char* text);

int core_websocket_recv_text(CoreWebSocketClient* ws, char* buf, size_t len);

void core_websocket_close(CoreWebSocketClient* ws);

#ifdef __cplusplus
}
#endif

#endif  // DISABLE_PEER_SIGNALING
#endif  // CORE_WEBSOCKET_H_
