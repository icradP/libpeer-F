/**
 * @file main.c
 * @brief ZLMediaKit P2P 适配示例
 *
 * 通过 WebSocket 连接 ZLMediaKit 信令服务器，实现 P2P 音视频通信
 *
 * ZLMediaKit P2P 信令协议 (WebSocket JSON):
 *   1. register: 注册到信令服务器，获取 room_id 和 ICE 服务器信息
 *   2. call: 发起呼叫 (SDP offer)
 *   3. accept: 接受呼叫 (SDP answer)
 *   4. candidate: 交换 ICE candidate
 *   5. bye: 挂断
 *
 * 流程:
 *   设备 A (推流端):
 *     1. WebSocket 连接信令服务器
 *     2. register 获取 room_id
 *     3. 创建 SDP offer
 *     4. call 发送 offer 给设备 B
 *     5. 接收 accept 中的 SDP answer
 *     6. 交换 ICE candidate
 *     7. P2P 连接建立，发送音视频
 *
 *   设备 B (拉流端):
 *     1. WebSocket 连接信令服务器
 *     2. register 获取 room_id
 *     3. 接收 call 中的 SDP offer
 *     4. 创建 SDP answer
 *     5. accept 发送 answer 给设备 A
 *     6. 交换 ICE candidate
 *     7. P2P 连接建立，接收音视频
 *
 * 编译前配置:
 *   修改 src/config.h: CONFIG_KEEPALIVE_TIMEOUT 改为 0
 *
 * 用法:
 *   # 推流端 (设备 A)
 *   ./zlm_p2p -H 192.168.1.100 -p 3000 -a live -s camera1 --push
 *
 *   # 拉流端 (设备 B)
 *   ./zlm_p2p -H 192.168.1.100 -p 3000 -a live -s camera1 --play
 */

#include <pthread.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <unistd.h>

#include "peer.h"

// ============================================================
// ZLMediaKit 信令协议常量
// ============================================================
#define ZLM_CLASS_KEY        "class"
#define ZLM_METHOD_KEY       "method"
#define ZLM_TRANSACTION_ID   "transactionId"
#define ZLM_ROOM_ID          "roomId"
#define ZLM_GUEST_ID         "guestId"
#define ZLM_TYPE_KEY         "type"
#define ZLM_SDP_KEY          "sdp"
#define ZLM_VHOST_KEY        "vhost"
#define ZLM_APP_KEY          "app"
#define ZLM_STREAM_KEY       "stream"
#define ZLM_CANDIDATE_KEY    "candidate"
#define ZLM_UFRAG_KEY        "ufrag"
#define ZLM_PWD_KEY          "pwd"
#define ZLM_ICE_SERVERS_KEY  "iceServers"
#define ZLM_URL_KEY          "url"

#define ZLM_CLASS_REQUEST    "request"
#define ZLM_CLASS_ACCEPT     "accept"
#define ZLM_CLASS_REJECT     "reject"
#define ZLM_CLASS_INDICATION "indication"

#define ZLM_METHOD_REGISTER   "register"
#define ZLM_METHOD_UNREGISTER "unregister"
#define ZLM_METHOD_CALL       "call"
#define ZLM_METHOD_BYE        "bye"
#define ZLM_METHOD_CANDIDATE  "candidate"

#define ZLM_TYPE_PLAY  "play"
#define ZLM_TYPE_PUSH  "push"

// ============================================================
// 全局状态
// ============================================================
static int g_interrupted = 0;
static PeerConnection* g_pc = NULL;
static PeerConnectionState g_state = PEER_CONNECTION_CLOSED;

// 信令状态
static char g_room_id[128] = {0};
static char g_guest_id[128] = {0};
static char g_transaction_id[64] = {0};
static int g_registered = 0;
static int g_is_push = 0;  // 1=推流端, 0=拉流端

// WebSocket 连接 (简化版，实际应使用 libwebsockets 或类似库)
// 这里用 HTTP POST 模拟信令交互
static char g_signaling_host[256] = {0};
static int g_signaling_port = 3000;
static char g_app[64] = {0};
static char g_stream[128] = {0};

// ============================================================
// 信令消息构建
// ============================================================

/**
 * 生成随机 transaction ID
 */
static void generate_transaction_id(char* buf, size_t len) {
  const char chars[] = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
  for (size_t i = 0; i < len - 1; i++) {
    buf[i] = chars[rand() % (sizeof(chars) - 1)];
  }
  buf[len - 1] = '\0';
}

/**
 * 构建 register 请求 JSON
 */
static char* build_register_request(void) {
  char* json = malloc(512);
  generate_transaction_id(g_transaction_id, sizeof(g_transaction_id));
  snprintf(json, 512,
    "{"
    "\"%s\":\"%s\","
    "\"%s\":\"%s\","
    "\"%s\":\"%s\","
    "\"%s\":\"\""
    "}",
    ZLM_CLASS_KEY, ZLM_CLASS_REQUEST,
    ZLM_METHOD_KEY, ZLM_METHOD_REGISTER,
    ZLM_TRANSACTION_ID, g_transaction_id,
    ZLM_ROOM_ID
  );
  return json;
}

/**
 * 构建 call 请求 JSON (SDP offer)
 */
static char* build_call_request(const char* sdp, const char* peer_room_id) {
  char* json = malloc(4096);
  generate_transaction_id(g_transaction_id, sizeof(g_transaction_id));
  snprintf(json, 4096,
    "{"
    "\"%s\":\"%s\","
    "\"%s\":\"%s\","
    "\"%s\":\"%s\","
    "\"%s\":\"%s\","
    "\"%s\":\"%s\","
    "\"%s\":\"%s\","
    "\"%s\":\"%s\","
    "\"%s\":\"%s\","
    "\"%s\":\"%s\","
    "\"%s\":\"%s\""
    "}",
    ZLM_CLASS_KEY, ZLM_CLASS_REQUEST,
    ZLM_METHOD_KEY, ZLM_METHOD_CALL,
    ZLM_TRANSACTION_ID, g_transaction_id,
    ZLM_GUEST_ID, g_guest_id,
    ZLM_ROOM_ID, peer_room_id,
    ZLM_VHOST_KEY, "__defaultVhost__",
    ZLM_APP_KEY, g_app,
    ZLM_STREAM_KEY, g_stream,
    ZLM_TYPE_KEY, g_is_push ? ZLM_TYPE_PUSH : ZLM_TYPE_PLAY,
    ZLM_SDP_KEY, sdp
  );
  return json;
}

/**
 * 构建 accept 响应 JSON (SDP answer)
 */
static char* build_accept_response(const char* sdp, const char* peer_room_id) {
  char* json = malloc(4096);
  snprintf(json, 4096,
    "{"
    "\"%s\":\"%s\","
    "\"%s\":\"%s\","
    "\"%s\":\"%s\","
    "\"%s\":\"%s\","
    "\"%s\":\"%s\","
    "\"%s\":\"%s\","
    "\"%s\":\"%s\","
    "\"%s\":\"%s\","
    "\"%s\":\"%s\""
    "}",
    ZLM_CLASS_KEY, ZLM_CLASS_ACCEPT,
    ZLM_METHOD_KEY, ZLM_METHOD_CALL,
    ZLM_TRANSACTION_ID, g_transaction_id,
    ZLM_GUEST_ID, g_guest_id,
    ZLM_ROOM_ID, peer_room_id,
    ZLM_VHOST_KEY, "__defaultVhost__",
    ZLM_APP_KEY, g_app,
    ZLM_STREAM_KEY, g_stream,
    ZLM_SDP_KEY, sdp
  );
  return json;
}

/**
 * 构建 candidate 消息 JSON
 */
static char* build_candidate_message(const char* candidate, const char* ufrag, const char* pwd, const char* peer_room_id) {
  char* json = malloc(2048);
  generate_transaction_id(g_transaction_id, sizeof(g_transaction_id));
  snprintf(json, 2048,
    "{"
    "\"%s\":\"%s\","
    "\"%s\":\"%s\","
    "\"%s\":\"%s\","
    "\"%s\":\"%s\","
    "\"%s\":\"%s\","
    "\"%s\":\"%s\","
    "\"%s\":\"%s\","
    "\"%s\":\"%s\","
    "\"%s\":\"%s\""
    "}",
    ZLM_CLASS_KEY, ZLM_CLASS_INDICATION,
    ZLM_METHOD_KEY, ZLM_METHOD_CANDIDATE,
    ZLM_TRANSACTION_ID, g_transaction_id,
    ZLM_GUEST_ID, g_guest_id,
    ZLM_ROOM_ID, peer_room_id,
    ZLM_CANDIDATE_KEY, candidate,
    ZLM_UFRAG_KEY, ufrag,
    ZLM_PWD_KEY, pwd
  );
  return json;
}

/**
 * 构建 bye 消息 JSON
 */
static char* build_bye_message(const char* peer_room_id) {
  char* json = malloc(512);
  generate_transaction_id(g_transaction_id, sizeof(g_transaction_id));
  snprintf(json, 512,
    "{"
    "\"%s\":\"%s\","
    "\"%s\":\"%s\","
    "\"%s\":\"%s\","
    "\"%s\":\"%s\","
    "\"%s\":\"%s\""
    "}",
    ZLM_CLASS_KEY, ZLM_CLASS_INDICATION,
    ZLM_METHOD_KEY, ZLM_METHOD_BYE,
    ZLM_TRANSACTION_ID, g_transaction_id,
    ZLM_GUEST_ID, g_guest_id,
    ZLM_ROOM_ID, peer_room_id
  );
  return json;
}

// ============================================================
// 信令消息处理 (需要对接 WebSocket)
// ============================================================

/**
 * 处理从信令服务器收到的消息
 * 这里是示例框架，实际需要 WebSocket 回调
 */
static void handle_signaling_message(const char* msg) {
  // TODO: 解析 JSON 并处理
  // 1. 如果是 register accept: 保存 room_id, 解析 ICE 服务器
  // 2. 如果是 call request: 解析 SDP offer, 创建 answer
  // 3. 如果是 call accept: 解析 SDP answer
  // 4. 如果是 candidate: 添加 ICE candidate
  // 5. 如果是 bye: 清理连接

  printf("[信令] 收到消息: %s\n", msg);
}

// ============================================================
// WebRTC 回调
// ============================================================

static void on_connection_state(PeerConnectionState state, void* data) {
  printf("[P2P] 连接状态: %s\n", peer_connection_state_to_string(state));
  g_state = state;
}

static void on_ice_candidate(char* sdp, void* userdata) {
  printf("[P2P] 收到 ICE candidate\n");
  // TODO: 通过 WebSocket 发送给对端
  // build_candidate_message(candidate, ufrag, pwd, peer_room_id);
}

// ============================================================
// 事件循环
// ============================================================

static void* peer_connection_task(void* data) {
  while (!g_interrupted) {
    peer_connection_loop((PeerConnection*)data);
    usleep(1000);
  }
  return NULL;
}

// ============================================================
// 主函数
// ============================================================

static void signal_handler(int sig) {
  g_interrupted = 1;
}

static void print_usage(const char* prog) {
  printf("用法: %s -H <host> -p <port> -a <app> -s <stream> [--push|--play]\n", prog);
  printf("\n参数:\n");
  printf("  -H <host>    ZLMediaKit 信令服务器地址\n");
  printf("  -p <port>    信令服务器端口 (默认 3000)\n");
  printf("  -a <app>     应用名\n");
  printf("  -s <stream>  流名\n");
  printf("  --push       推流端模式\n");
  printf("  --play       拉流端模式\n");
  printf("\n示例:\n");
  printf("  # 推流端\n");
  printf("  %s -H 192.168.1.100 -p 3000 -a live -s camera1 --push\n", prog);
  printf("  # 拉流端\n");
  printf("  %s -H 192.168.1.100 -p 3000 -a live -s camera1 --play\n", prog);
}

int main(int argc, char* argv[]) {
  g_signaling_port = 3000;

  // 解析参数
  for (int i = 1; i < argc; i++) {
    if (strcmp(argv[i], "-H") == 0 && i + 1 < argc) {
      strncpy(g_signaling_host, argv[++i], sizeof(g_signaling_host) - 1);
    } else if (strcmp(argv[i], "-p") == 0 && i + 1 < argc) {
      g_signaling_port = atoi(argv[++i]);
    } else if (strcmp(argv[i], "-a") == 0 && i + 1 < argc) {
      strncpy(g_app, argv[++i], sizeof(g_app) - 1);
    } else if (strcmp(argv[i], "-s") == 0 && i + 1 < argc) {
      strncpy(g_stream, argv[++i], sizeof(g_stream) - 1);
    } else if (strcmp(argv[i], "--push") == 0) {
      g_is_push = 1;
    } else if (strcmp(argv[i], "--play") == 0) {
      g_is_push = 0;
    } else {
      print_usage(argv[0]);
      return 1;
    }
  }

  if (!g_signaling_host[0] || !g_app[0] || !g_stream[0]) {
    print_usage(argv[0]);
    return 1;
  }

  signal(SIGINT, signal_handler);

  printf("========================================\n");
  printf(" ZLMediaKit P2P 示例 (libpeer)\n");
  printf("========================================\n");
  printf(" 信令服务器: %s:%d\n", g_signaling_host, g_signaling_port);
  printf(" 应用名:     %s\n", g_app);
  printf(" 流名:       %s\n", g_stream);
  printf(" 模式:       %s\n", g_is_push ? "推流端" : "拉流端");
  printf("========================================\n\n");

  // 初始化
  peer_init();

  // 创建 PeerConnection (仅音频)
  PeerConfiguration config = {
      .ice_servers = {
          {.urls = "stun:stun.l.google.com:19302"},
      },
      .datachannel = DATA_CHANNEL_NONE,
      .video_codec = CODEC_NONE,
      .audio_codec = CODEC_PCMA,
  };

  g_pc = peer_connection_create(&config);
  if (!g_pc) {
    printf("[错误] 创建 PeerConnection 失败\n");
    peer_deinit();
    return 1;
  }

  peer_connection_oniceconnectionstatechange(g_pc, on_connection_state);
  peer_connection_onicecandidate(g_pc, on_ice_candidate);

  // ============================================================
  // TODO: 实现 WebSocket 信令交互
  // ============================================================
  //
  // 1. 连接 WebSocket: ws://host:port/
  //
  // 2. 发送 register 请求:
  //    char* reg_msg = build_register_request();
  //    ws_send(reg_msg);
  //    free(reg_msg);
  //
  // 3. 收到 register accept 后:
  //    - 保存 room_id
  //    - 解析 ICE 服务器 (stun/turn)
  //    - 更新 PeerConfiguration
  //
  // 4. 推流端:
  //    - 创建 offer: peer_connection_create_offer(pc)
  //    - 发送 call 请求: build_call_request(sdp, peer_room_id)
  //    - 收到 accept: 解析 answer, set_remote_description
  //
  // 5. 拉流端:
  //    - 收到 call 请求: 解析 offer
  //    - 创建 answer: peer_connection_create_answer(pc)
  //    - 发送 accept: build_accept_response(sdp, peer_room_id)
  //
  // 6. 双方交换 ICE candidate
  //
  // 7. P2P 连接建立后:
  //    - 推流端: peer_connection_send_video/audio
  //    - 拉流端: onvideotrack/onaudiotrack 回调接收

  printf("[P2P] WebSocket 信令适配层待实现\n");
  printf("[P2P] 请参考 ZLMediaKit WebRtcSignalingSession.cpp 的协议格式\n");

  // 启动事件循环
  pthread_t pc_thread;
  pthread_create(&pc_thread, NULL, peer_connection_task, g_pc);

  uint64_t audio_time = 0;

  while (!g_interrupted) {
    if (g_state == PEER_CONNECTION_COMPLETED && g_is_push) {
      struct timeval tv;
      gettimeofday(&tv, NULL);
      uint64_t now = (uint64_t)tv.tv_sec * 1000 + tv.tv_usec / 1000;

      // 音频: 每 20ms 发一帧 PCMA (160 samples @ 8kHz)
      if (now - audio_time >= 20) {
        audio_time = now;
        uint8_t audio_buf[160];
        for (int i = 0; i < 160; i++) {
          audio_buf[i] = 0xD5; // PCMA silence
        }
        peer_connection_send_audio(g_pc, audio_buf, 160);
      }
    }
    usleep(1000);
  }

  // 清理
  printf("\n[P2P] 正在清理...\n");

  // 发送 bye
  // char* bye_msg = build_bye_message(peer_room_id);
  // ws_send(bye_msg);
  // free(bye_msg);

  pthread_join(pc_thread, NULL);
  peer_connection_destroy(g_pc);
  peer_deinit();

  printf("[P2P] 已退出\n");
  return 0;
}
