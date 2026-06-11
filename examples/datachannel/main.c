/**
 * @file main.c
 * @brief DataChannel 示例 — 双向数据通道通信
 *
 * 本示例展示 libpeer DataChannel 的完整用法:
 *   1. 创建 PeerConnection (含 DataChannel)
 *   2. 通过 HTTP 信令与 ZLMediaKit 建立连接
 *   3. 通过 DataChannel 双向发送/接收消息
 *
 * DataChannel vs RTP:
 *   - DataChannel: 基于 SCTP/DTLS, 用于文本/二进制数据 (类似 WebSocket)
 *   - RTP Track: 基于 RTP/DTLS, 用于音视频媒体流
 *
 * 重要配置 (编译前):
 *   ZLMediaKit 不发送 STUN keepalive 包，需要关闭 libpeer 的 keepalive 检测:
 *   修改 src/config.h: CONFIG_KEEPALIVE_TIMEOUT 改为 0
 *   #define CONFIG_KEEPALIVE_TIMEOUT 0
 *
 * 用法:
 *   ./datachannel -u http://192.168.1.100:80/index/api/whip?app=live&stream=test_dc [-t token]
 *
 * 注意:
 *   - ZLMediaKit 的 WHIP 端点主要用于音视频推流
 *   - DataChannel 需要 ZLMediaKit 支持 SCTP 数据通道
 *   - 如果只需要 DataChannel，可以不配置音视频编码器
 */

#include <pthread.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "peer.h"

static int g_interrupted = 0;
static PeerConnection* g_pc = NULL;
static PeerConnectionState g_state = PEER_CONNECTION_CLOSED;
static int g_dc_open = 0;
static int g_msg_received = 0;

static void signal_handler(int sig) {
  g_interrupted = 1;
}

/**
 * ICE 连接状态变化回调
 */
static void on_connection_state(PeerConnectionState state, void* data) {
  printf("[DC] 连接状态: %s\n", peer_connection_state_to_string(state));
  g_state = state;

  if (state == PEER_CONNECTION_COMPLETED) {
    printf("[DC] 连接已建立, DataChannel 就绪\n");
  }
}

/**
 * DataChannel 打开回调
 * 当 DataChannel 成功建立时触发, 此时可以发送消息
 */
static void on_datachannel_open(void* user_data) {
  printf("[DC] DataChannel 已打开\n");
  g_dc_open = 1;
}

/**
 * DataChannel 关闭回调
 */
static void on_datachannel_close(void* user_data) {
  printf("[DC] DataChannel 已关闭\n");
  g_dc_open = 0;
}

/**
 * DataChannel 消息接收回调
 * 当通过 DataChannel 收到消息时触发
 */
static void on_datachannel_message(char* msg, size_t len, void* user_data, uint16_t sid) {
  printf("[DC] 收到消息 (SID=%d, %zu字节): %.*s\n", sid, len, (int)len, msg);
  g_msg_received = 1;

  // Echo: 将收到的消息发回去
  if (g_dc_open) {
    char reply[512];
    snprintf(reply, sizeof(reply), "echo: %.*s", (int)len, msg);
    peer_connection_datachannel_send(g_pc, reply, strlen(reply));
    printf("[DC] 已回复: %s\n", reply);
  }
}

/**
 * PeerConnection 事件循环线程
 */
static void* peer_connection_task(void* data) {
  while (!g_interrupted) {
    peer_connection_loop((PeerConnection*)data);
    usleep(1000);
  }
  return NULL;
}

static void print_usage(const char* prog) {
  printf("用法: %s -u <url> [-t <token>]\n", prog);
  printf("\n参数:\n");
  printf("  -u <url>    信令服务器 URL\n");
  printf("              示例: http://192.168.1.100:80/index/api/whip?app=live&stream=test_dc\n");
  printf("  -t <token>  Bearer Token (可选)\n");
  printf("\n功能:\n");
  printf("  - 建立 DataChannel 连接\n");
  printf("  - 每秒发送一条消息\n");
  printf("  - 接收并 echo 回复消息\n");
}

int main(int argc, char* argv[]) {
  const char* url = NULL;
  const char* token = NULL;

  // 解析参数
  for (int i = 1; i < argc; i++) {
    if (strcmp(argv[i], "-u") == 0 && i + 1 < argc) {
      url = argv[++i];
    } else if (strcmp(argv[i], "-t") == 0 && i + 1 < argc) {
      token = argv[++i];
    } else {
      print_usage(argv[0]);
      return 1;
    }
  }

  if (!url) {
    print_usage(argv[0]);
    return 1;
  }

  signal(SIGINT, signal_handler);

  printf("========================================\n");
  printf(" DataChannel 示例 (libpeer)\n");
  printf("========================================\n");
  printf(" URL:   %s\n", url);
  printf(" Token: %s\n", token ? token : "(无)");
  printf("========================================\n\n");

  // 初始化
  peer_init();

  // 创建 PeerConnection
  // DataChannel 模式: DATA_CHANNEL_STRING (文本消息)
  // 同时配置音视频 (ZLMediaKit 需要至少一个媒体 track 才能建立连接)
  PeerConfiguration config = {
      .ice_servers = {
          {.urls = "stun:stun.l.google.com:19302"},
      },
      .datachannel = DATA_CHANNEL_STRING,  // 启用 DataChannel
      .video_codec = CODEC_H264,            // ZLMediaKit 需要视频 track
      .audio_codec = CODEC_NONE,            // 不需要音频
  };

  g_pc = peer_connection_create(&config);
  if (!g_pc) {
    printf("[错误] 创建 PeerConnection 失败\n");
    peer_deinit();
    return 1;
  }

  // 注册回调
  peer_connection_oniceconnectionstatechange(g_pc, on_connection_state);
  peer_connection_ondatachannel(g_pc, on_datachannel_message, on_datachannel_open, on_datachannel_close);

  // 连接信令服务器
  printf("[DC] 正在连接 %s ...\n", url);
  if (peer_signaling_connect(url, token, g_pc) != 0) {
    printf("[错误] 连接失败\n");
    peer_connection_destroy(g_pc);
    peer_deinit();
    return 1;
  }

  // 启动事件循环线程
  pthread_t pc_thread;
  pthread_create(&pc_thread, NULL, peer_connection_task, g_pc);

  int msg_count = 0;

  // 主循环
  while (!g_interrupted) {
    // 连接建立后, 创建 DataChannel 并发送消息
    if (g_state == PEER_CONNECTION_COMPLETED && !g_dc_open) {
      // 主动创建 DataChannel (offerer 端)
      int ret = peer_connection_create_datachannel(
          g_pc,
          DATA_CHANNEL_RELIABLE,  // 可靠有序传输
          0, 0,                    // 无优先级限制
          "libpeer-dc",           // DataChannel 标签
          ""                       // 协议 (可选)
      );
      if (ret > 0) {
        printf("[DC] DataChannel 已创建 (SID=%d)\n", ret);
        g_dc_open = 1;
      }
    }

    // 定期发送消息
    if (g_dc_open) {
      char msg[256];
      snprintf(msg, sizeof(msg), "Hello from libpeer #%d", msg_count++);
      int ret = peer_connection_datachannel_send(g_pc, msg, strlen(msg));
      if (ret == 0) {
        printf("[DC] 已发送: %s\n", msg);
      }
      sleep(1);
    } else {
      usleep(100000);
    }
  }

  printf("\n[DC] 正在清理...\n");

  // 清理
  peer_signaling_disconnect();
  pthread_join(pc_thread, NULL);
  peer_connection_destroy(g_pc);
  peer_deinit();

  printf("[DC] 已退出, 共发送 %d 条消息, 收到 %d 条回复\n", msg_count, g_msg_received);
  return 0;
}
