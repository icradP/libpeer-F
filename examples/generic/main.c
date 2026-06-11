#include <pthread.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/time.h>
#include <unistd.h>

#include "peer.h"
#include "reader.h"

int g_interrupted = 0;
PeerConnection* g_pc = NULL;
PeerConnectionState g_state;

/**
 * ICE 连接状态变化回调
 * WebRTC 交互: 当 PeerConnection 的 ICE 连接状态发生变化时触发
 * 状态流转: new → checking → connected → completed → disconnected → failed → closed
 */
static void onconnectionstatechange(PeerConnectionState state, void* data) {
  printf("state is changed: %s\n", peer_connection_state_to_string(state));
  g_state = state;
}

/**
 * DataChannel 打开回调
 * WebRTC 交互 (DataChannel): 当 DataChannel 成功建立时触发
 * 此时双方可以通过 DataChannel 进行双向数据传输
 */
static void onopen(void* user_data) {
}

/**
 * DataChannel 关闭回调
 * WebRTC 交互 (DataChannel): 当 DataChannel 关闭时触发
 * 通常发生在 PeerConnection 关闭或网络断开时
 */
static void onclose(void* user_data) {
}

/**
 * DataChannel 消息接收回调
 * WebRTC 交互 (DataChannel): 当通过 DataChannel 收到消息时触发
 * DataChannel 基于 SCTP/DTLS 协议，用于传输任意数据（非媒体流）
 * 适用于文本消息、文件传输、游戏状态同步等场景
 *
 * 本示例中用于 ping/pong 心跳测试，验证 DataChannel 双向通信是否正常
 */
static void onmessage(char* msg, size_t len, void* user_data, uint16_t sid) {
  printf("on message: %d %.*s", sid, (int)len, msg);

  if (strncmp(msg, "ping", 4) == 0) {
    printf(", send pong\n");
    /**
     * WebRTC 交互 (DataChannel): 通过 DataChannel 发送 pong 响应
     * 这是双向数据通信，不经过媒体通道
     */
    peer_connection_datachannel_send(g_pc, "pong", 4);
  }
}

static void signal_handler(int signal) {
  g_interrupted = 1;
}

/**
 * 信令服务事件循环线程
 * WebRTC 交互 (信令): 信令服务器负责交换 SDP 和 ICE candidate
 * 信令协议通常基于 WebSocket，用于:
 *   1. 交换 SDP Offer/Answer（会话描述）
 *   2. 交换 ICE candidate（网络连通性信息）
 *   3. 房间管理、用户发现等
 */
static void* peer_singaling_task(void* data) {
  while (!g_interrupted) {
    peer_signaling_loop();
    usleep(1000);
  }

  pthread_exit(NULL);
}

/**
 * PeerConnection 事件循环线程
 * WebRTC 交互: 处理 WebRTC 协议栈的所有底层操作:
 *   - ICE candidate 收集与连接检查（NAT 穿透）
 *   - DTLS 握手（安全传输层）
 *   - SCTP 协商（DataChannel 协议）
 *   - RTP/RTCP 媒体数据收发（音视频流）
 */
static void* peer_connection_task(void* data) {
  while (!g_interrupted) {
    peer_connection_loop(g_pc);
    usleep(1000);
  }

  pthread_exit(NULL);
}

static uint64_t get_timestamp() {
  struct timeval tv;
  gettimeofday(&tv, NULL);
  return tv.tv_sec * 1000 + tv.tv_usec / 1000;
}

void print_usage(const char* prog_name) {
  printf("Usage: %s -u <url> [-t <token>]\n", prog_name);
}

void parse_arguments(int argc, char* argv[], const char** url, const char** token) {
  *token = NULL;
  *url = NULL;

  for (int i = 1; i < argc; i++) {
    if (strcmp(argv[i], "-u") == 0 && (i + 1) < argc) {
      *url = argv[++i];
    } else if (strcmp(argv[i], "-t") == 0 && (i + 1) < argc) {
      *token = argv[++i];
    } else {
      print_usage(argv[0]);
      exit(1);
    }
  }

  if (*url == NULL) {
    print_usage(argv[0]);
    exit(1);
  }
}

int main(int argc, char* argv[]) {
  uint64_t curr_time, video_time, audio_time;
  uint8_t* buf = NULL;
  const char* url = NULL;
  const char* token = NULL;
  int size;

  pthread_t peer_singaling_thread;
  pthread_t peer_connection_thread;

  parse_arguments(argc, argv, &url, &token);

  signal(SIGINT, signal_handler);

  /**
   * WebRTC 配置
   * - ice_servers: STUN/TURN 服务器，用于 NAT 穿透和公网 IP 发现
   * - datachannel: 启用 DataChannel（WebRTC 数据通道，基于 SCTP/DTLS）
   * - video_codec: 视频编解码器（H.264，用于 RTP 媒体流）
   * - audio_codec: 音频编解码器（PCMA/G.711 A-law，用于 RTP 媒体流）
   *
   * 注意: 本示例同时使用了 DataChannel 和 RTP Track 两种数据传输方式
   */
  PeerConfiguration config = {
      .ice_servers = {
          {.urls = "stun:stun.l.google.com:19302"},
      },
      .datachannel = DATA_CHANNEL_STRING,
      .video_codec = CODEC_H264,
      .audio_codec = CODEC_PCMA};

  printf("=========== Parsed Arguments ===========\n");
  printf(" %-5s : %s\n", "URL", url);
  printf(" %-5s : %s\n", "Token", token ? token : "");
  printf("========================================\n");

  /**
   * 初始化 WebRTC 库并创建 PeerConnection
   * WebRTC 交互: PeerConnection 是 WebRTC 的核心对象，管理整个连接生命周期
   * 包含两种数据通道:
   *   1. RTP Track: 音视频媒体流（H.264 视频 + PCMA 音频）
   *   2. DataChannel: 任意数据传输（ping/pong 消息）
   */
  peer_init();
  g_pc = peer_connection_create(&config);

  /**
   * 注册回调函数
   * WebRTC 交互:
   *   - oniceconnectionstatechange: 监听 ICE 连接状态变化（RTP 和 DataChannel 共享）
   *   - ondatachannel: 监听 DataChannel 消息（仅 DataChannel，非 RTP）
   */
  peer_connection_oniceconnectionstatechange(g_pc, onconnectionstatechange);
  peer_connection_ondatachannel(g_pc, onmessage, onopen, onclose);

  /**
   * 连接信令服务器
   * WebRTC 交互 (信令): 信令服务器是 WebRTC 连接建立的前提
   * 通过 WebSocket 连接到信令服务器，用于:
   *   1. 房间加入/退出
   *   2. SDP Offer/Answer 交换（包含媒体能力和 DataChannel 能力）
   *   3. ICE candidate 交换
   */
  peer_signaling_connect(url, token, g_pc);

  /**
   * 启动事件循环线程
   * WebRTC 交互: WebRTC 需要两个独立的事件循环:
   *   1. peer_connection_thread: 处理 ICE/DTLS/SCTP/RTP 协议栈
   *   2. peer_singaling_thread: 处理 WebSocket 信令消息
   */
  pthread_create(&peer_connection_thread, NULL, peer_connection_task, NULL);
  pthread_create(&peer_singaling_thread, NULL, peer_singaling_task, NULL);

  /**
   * 初始化媒体读取器
   * 从文件读取 H.264 视频和 PCMA 音频数据
   * 这些数据将通过 RTP Track 发送，而非 DataChannel
   */
  reader_init();

  /**
   * 主循环: 发送媒体数据和 DataChannel 消息
   * WebRTC 交互: 本示例使用两种并行的数据传输方式:
   *   1. RTP Track: 发送音视频媒体流（peer_connection_send_video/audio）
   *   2. DataChannel: 发送/接收控制消息（ping/pong）
   */
  while (!g_interrupted) {
    if (g_state == PEER_CONNECTION_COMPLETED) {
      curr_time = get_timestamp();

      /**
       * 【RTP Track】发送视频帧（25 FPS）
       * WebRTC 交互 (RTP): 视频数据通过 RTP 协议发送
       * H.264 视频需要封装为 RTP 包（RFC 6184）
       * 关键帧（IDR）需要包含 SPS/PPS 参数集
       * 这是媒体流传输，走的是 RTP 协议栈
       */
      if (curr_time - video_time > 40) {
        video_time = curr_time;
        if ((buf = reader_get_video_frame(&size)) != NULL) {
          peer_connection_send_video(g_pc, buf, size);
          free(buf);
          buf = NULL;
        }
      }

      /**
       * 【RTP Track】发送音频帧（50 FPS，每帧 20ms）
       * WebRTC 交互 (RTP): 音频数据通过 RTP 协议发送
       * PCMA (G.711 A-law) 编码，采样率 8kHz，单声道
       * 每帧 160 采样 = 20ms
       * 这是媒体流传输，走的是 RTP 协议栈
       */
      if (curr_time - audio_time > 20) {
        if ((buf = reader_get_audio_frame(&size)) != NULL) {
          peer_connection_send_audio(g_pc, buf, size);
          buf = NULL;
        }
        audio_time = curr_time;
      }
    }
    usleep(1000);
  }

  /**
   * 清理资源
   * WebRTC 交互: 关闭所有 WebRTC 连接和资源
   *   1. 等待事件循环线程退出
   *   2. 断开信令服务器连接
   *   3. 销毁 PeerConnection（关闭 ICE/DTLS/SCTP/RTP 连接）
   *   4. 反初始化 WebRTC 库
   */
  pthread_join(peer_singaling_thread, NULL);
  pthread_join(peer_connection_thread, NULL);

  reader_deinit();

  peer_signaling_disconnect();
  peer_connection_destroy(g_pc);
  peer_deinit();

  return 0;
}
