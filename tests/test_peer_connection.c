#include <pthread.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

#include "peer.h"

#define MAX_CONNECTION_ATTEMPTS 25
#define OFFER_DATACHANNEL_MESSAGE "Hello World"
#define ANSWER_DATACHANNEL_MESSAGE "Foobar"
#define DATACHANNEL_NAME "libpeer-datachannel"

int test_complete = 0;

typedef struct {
  PeerConnection *offer_peer_connection, *answer_peer_connection;
  int onmessage_offer_called, onmessage_answer_called, test_complete;
} TestUserData;

/**
 * Offerer 连接状态变化回调
 * WebRTC 交互: 当 offerer 端的 ICE 连接状态发生变化时触发
 * 状态包括: new → checking → connected → completed → disconnected → failed → closed
 */
static void onconnectionstatechange_offerer_peer_connection(PeerConnectionState state, void* user_data) {
  printf("offer state is changed: %s\n", peer_connection_state_to_string(state));
}

/**
 * Answerer 连接状态变化回调
 * WebRTC 交互: 当 answerer 端的 ICE 连接状态发生变化时触发
 */
static void onconnectionstatechange_answerer_peer_connection(PeerConnectionState state, void* user_data) {
  printf("answerer state is changed: %s\n", peer_connection_state_to_string(state));
}

/**
 * Offerer ICE candidate 回调
 * WebRTC 交互: 当 offerer 端发现新的 ICE candidate 时触发
 * 在 trickle ICE 模式下，candidate 会通过信令服务器发送给对端
 * 本测试使用内联 candidate（非 trickle），所以回调为空
 */
static void onicecandidate_offerer_peer_connection(char* description, void* user_data) {
}

/**
 * Answerer ICE candidate 回调
 * WebRTC 交互: 当 answerer 端发现新的 ICE candidate 时触发
 * 同样使用内联 candidate，回调为空
 */
static void onicecandidate_answerer_peer_connection(char* description, void* user_data) {
}

/**
 * Offerer DataChannel 消息接收回调
 * WebRTC 交互: 当 offerer 端通过 DataChannel 收到消息时触发
 * DataChannel 是 WebRTC 中用于传输任意数据的通道（类似 WebSocket，但基于 SCTP/DTLS）
 */
static void ondatachannel_onmessage_offerer_peer_connection(char* msg, size_t len, void* userdata, uint16_t sid) {
  TestUserData* test_user_data = (TestUserData*)userdata;

  if (strcmp(msg, ANSWER_DATACHANNEL_MESSAGE) == 0) {
    test_user_data->onmessage_offer_called = 1;
  }
}

/**
 * Answerer DataChannel 消息接收回调
 * WebRTC 交互: 当 answerer 端通过 DataChannel 收到消息时触发
 */
static void ondatachannel_onmessage_answerer_peer_connection(char* msg, size_t len, void* userdata, uint16_t sid) {
  TestUserData* test_user_data = (TestUserData*)userdata;

  if (strcmp(msg, OFFER_DATACHANNEL_MESSAGE) == 0) {
    test_user_data->onmessage_answer_called = 1;
  }
}

/**
 * PeerConnection 事件循环线程
 * WebRTC 交互: 每个 PeerConnection 需要独立的事件循环来处理:
 *   - ICE candidate 收集与连接检查
 *   - DTLS 握手
 *   - SCTP 协商（用于 DataChannel）
 *   - 媒体数据收发
 */
static void* peer_connection_task(void* user_data) {
  PeerConnection* peer_connection = (PeerConnection*)user_data;

  while (!test_complete) {
    peer_connection_loop(peer_connection);
    usleep(1000);
  }

  pthread_exit(NULL);
  return NULL;
}

int main(int argc, char* argv[]) {
  pthread_t offer_thread, answer_thread;

  TestUserData test_user_data = {
      .offer_peer_connection = NULL,
      .answer_peer_connection = NULL,
  };

  /**
   * WebRTC 配置
   * - ice_servers: STUN/TURN 服务器，用于 NAT 穿透和公网 IP 发现
   * - datachannel: 启用 DataChannel（WebRTC 数据通道）
   * - video_codec: 视频编解码器（H.264）
   * - audio_codec: 音频编解码器（Opus）
   */
  PeerConfiguration config = {
      .ice_servers = {
          {.urls = "stun:stun.l.google.com:19302"},
      },
      .datachannel = DATA_CHANNEL_STRING,
      .video_codec = CODEC_H264,
      .audio_codec = CODEC_OPUS,
      .user_data = &test_user_data,
  };

  /* 初始化 WebRTC 库 */
  peer_init();

  /**
   * 创建 Offerer 和 Answerer 两个 PeerConnection
   * WebRTC 交互: 这是 WebRTC 连接的起点，双方各自创建 PeerConnection 实例
   */
  test_user_data.offer_peer_connection = peer_connection_create(&config);
  test_user_data.answer_peer_connection = peer_connection_create(&config);

  /**
   * 注册 ICE 连接状态变化回调
   * WebRTC 交互: 监听 ICE 连接生命周期状态变化
   * 状态流转: new → checking → connected → completed
   */
  peer_connection_oniceconnectionstatechange(test_user_data.offer_peer_connection, onconnectionstatechange_offerer_peer_connection);
  peer_connection_oniceconnectionstatechange(test_user_data.answer_peer_connection, onconnectionstatechange_answerer_peer_connection);

  /**
   * 注册 ICE candidate 回调
   * WebRTC 交互: 在 trickle ICE 模式下，每当发现新 candidate 就通知应用层
   * 应用层需通过信令服务器将 candidate 发送给对端
   */
  peer_connection_onicecandidate(test_user_data.offer_peer_connection, onicecandidate_offerer_peer_connection);
  peer_connection_onicecandidate(test_user_data.answer_peer_connection, onicecandidate_answerer_peer_connection);

  /**
   * 注册 DataChannel 消息回调
   * WebRTC 交互: DataChannel 建立后，双方可通过此通道进行低延迟的双向数据传输
   * 常用于聊天、文件传输、游戏状态同步等场景
   */
  peer_connection_ondatachannel(test_user_data.offer_peer_connection, ondatachannel_onmessage_offerer_peer_connection, NULL, NULL);
  peer_connection_ondatachannel(test_user_data.answer_peer_connection, ondatachannel_onmessage_answerer_peer_connection, NULL, NULL);

  /**
   * 启动事件循环线程
   * WebRTC 交互: 每个 PeerConnection 需要独立线程运行事件循环
   * 负责处理 ICE、DTLS、SCTP 等底层协议栈
   */
  pthread_create(&offer_thread, NULL, peer_connection_task, test_user_data.offer_peer_connection);
  pthread_create(&answer_thread, NULL, peer_connection_task, test_user_data.answer_peer_connection);

  /**
   * ========== SDP 信令交换 ==========
   * WebRTC 交互: 这是 WebRTC 连接建立的核心步骤（信令阶段）
   *
   * 1. Offerer 创建 SDP Offer（包含媒体能力、ICE candidate 等）
   * 2. Offer 将通过信令服务器发送给 Answerer（本测试直接传递）
   * 3. Answerer 设置 Offer 为远程描述，创建 SDP Answer
   * 4. Answer 通过信令服务器返回给 Offerer（本测试直接传递）
   * 5. Offerer 设置 Answer 为远程描述
   *
   * 完成后，双方进入 ICE 连接检查阶段
   */
  const char* offer = peer_connection_create_offer(test_user_data.offer_peer_connection);
  peer_connection_set_remote_description(test_user_data.answer_peer_connection, offer, SDP_TYPE_OFFER);
  const char* answer = peer_connection_create_answer(test_user_data.answer_peer_connection);
  peer_connection_set_remote_description(test_user_data.offer_peer_connection, answer, SDP_TYPE_ANSWER);

  /**
   * 等待 ICE 连接建立并测试 DataChannel
   * WebRTC 交互:
   *   1. ICE 连接检查完成后，状态变为 PEER_CONNECTION_COMPLETED
   *   2. Offerer 创建 DataChannel（SCTP 协议建立数据通道）
   *   3. 双方通过 DataChannel 交换消息验证连通性
   */
  int attempts = 0, datachannel_created = 0;
  while (attempts < MAX_CONNECTION_ATTEMPTS) {
    /**
     * 当 ICE 连接完成（PEER_CONNECTION_COMPLETED）后，创建 DataChannel
     * WebRTC 交互: DataChannel 基于 SCTP 协议，需要在 ICE/DTLS 握手完成后建立
     * 参数: DATA_CHANNEL_RELIABLE = 可靠有序传输（类似 TCP）
     *       0, 0 = 无重传次数和优先级限制
     */
    if (!datachannel_created && peer_connection_get_state(test_user_data.offer_peer_connection) == PEER_CONNECTION_COMPLETED) {
      if (peer_connection_create_datachannel(test_user_data.offer_peer_connection, DATA_CHANNEL_RELIABLE, 0, 0, DATACHANNEL_NAME, "bar") == 18) {
        datachannel_created = 1;
      }
    }

    /**
     * 检查双方连接是否完成且消息收发是否成功
     * WebRTC 交互: 验证完整的 WebRTC 连接流程
     *   - ICE 连接建立 ✓
     *   - DTLS 握手完成 ✓
     *   - SCTP 协商完成 ✓
     *   - DataChannel 消息双向传输 ✓
     */
    if (peer_connection_get_state(test_user_data.offer_peer_connection) == PEER_CONNECTION_COMPLETED &&
        peer_connection_get_state(test_user_data.answer_peer_connection) == PEER_CONNECTION_COMPLETED &&
        test_user_data.onmessage_offer_called == 1 &&
        test_user_data.onmessage_answer_called == 1) {
      break;
    }

    /**
     * 双方通过 DataChannel 发送消息
     * WebRTC 交互: DataChannel 提供低延迟的双向数据传输
     * 类似 WebSocket，但基于 SCTP/DTLS，更安全且支持 NAT 穿透
     */
    peer_connection_datachannel_send(test_user_data.offer_peer_connection, OFFER_DATACHANNEL_MESSAGE, sizeof(OFFER_DATACHANNEL_MESSAGE));
    peer_connection_datachannel_send(test_user_data.answer_peer_connection, ANSWER_DATACHANNEL_MESSAGE, sizeof(ANSWER_DATACHANNEL_MESSAGE));

    attempts++;
    usleep(250000);
  }

  /**
   * 验证 DataChannel 标签映射
   * WebRTC 交互: DataChannel 通过 SID（Stream ID）标识，验证标签正确映射
   */
  if (strcmp(DATACHANNEL_NAME, peer_connection_lookup_sid_label(test_user_data.answer_peer_connection, 0)) != 0) {
    return 1;
  }

  /**
   * 清理资源
   * WebRTC 交互: 关闭 PeerConnection，释放所有底层资源
   * 包括: ICE 连接、DTLS 会话、SCTP 关联等
   */
  test_complete = 1;
  pthread_join(offer_thread, NULL);
  pthread_join(answer_thread, NULL);
  peer_connection_destroy(test_user_data.offer_peer_connection);
  peer_connection_destroy(test_user_data.answer_peer_connection);

  peer_deinit();
  return attempts == MAX_CONNECTION_ATTEMPTS ? 1 : 0;
}
