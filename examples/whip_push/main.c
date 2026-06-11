/**
 * @file main.c
 * @brief WHIP 推流示例 — 设备通过 libpeer HTTP 信令向 ZLMediaKit 推送音视频
 *
 * 流程:
 *   1. 创建 PeerConnection (按需启用 H264 视频 / PCMA 音频)
 *   2. 通过 HTTP POST 发送 SDP offer 到 ZLMediaKit WHIP 端点
 *   3. 接收 201 响应中的 SDP answer 兼容200响应 :json格式和字符串格式
 *   4. ICE 连通性检查 → DTLS 握手 → RTP 推流
 *
 * 重要配置:
 *   whip server服务 answer 如果是 ICE-lite，可能不持续响应 STUN。创建 PeerConnection 时设置:
 *     .skip_stun_check_keepalive = 1
 *   仍会发送一次 STUN 提名，但不等待响应、不因 keepalive 超时断开。
 *
 * HTTPS 要求:
 *   WebRTC 安全模型要求 HTTPS。如果用 HTTP，浏览器会拒绝。
 *   设备端 (非浏览器) 可以用 HTTP，但建议用 HTTPS。
 *
 * 用法:
 *   ./whip_push -u http://192.168.1.100:80/index/api/whip?app=live&stream=camera1 [-t token]
 *   ./whip_push -u https://192.168.1.100:443/index/api/whip?app=live&stream=camera1 -v test.h264
 *
 * 对接 ZLMediaKit:
 *   - 服务器启动后，设备直接运行本程序
 *   - 推流成功后可通过以下地址拉流:
 *     RTMP: rtmp://server:1935/live/camera1
 *     HTTP-FLV: http://server:80/live/camera1.live.flv
 *     WS-FLV: ws://server:80/live/camera1.live.flv
 *     WebRTC: http://server:80/index/api/webrtc?app=live&stream=camera1
 */

#include <pthread.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <unistd.h>

#include "peer.h"

static int g_interrupted = 0;
static PeerConnection* g_pc = NULL;
static PeerConnectionState g_state = PEER_CONNECTION_CLOSED;

static void signal_handler(int sig) {
  g_interrupted = 1;
}

/**
 * ICE 连接状态变化回调
 */
static void on_connection_state(PeerConnectionState state, void* data) {
  printf("[WHIP] 连接状态: %s\n", peer_connection_state_to_string(state));
  g_state = state;
}

/**
 * 生成测试用 H264 关键帧 (简化版 SPS+PPS+IDR)
 * 实际设备应从摄像头或编码器获取
 */
static int generate_test_h264_frame(uint8_t* buf, int is_keyframe) {
  int offset = 0;

  if (is_keyframe) {
    // SPS (简化)
    uint8_t sps[] = {0x00, 0x00, 0x00, 0x01, 0x67, 0x42, 0xe0, 0x1f, 0xda, 0x02, 0x80, 0xf6, 0x80};
    memcpy(buf + offset, sps, sizeof(sps));
    offset += sizeof(sps);

    // PPS (简化)
    uint8_t pps[] = {0x00, 0x00, 0x00, 0x01, 0x68, 0xce, 0x38, 0x80};
    memcpy(buf + offset, pps, sizeof(pps));
    offset += sizeof(pps);
  }

  // IDR/P-frame NAL (简化，实际应为编码器输出)
  uint8_t nal_header[] = {0x00, 0x00, 0x00, 0x01};
  memcpy(buf + offset, nal_header, 4);
  offset += 4;
  buf[offset++] = is_keyframe ? 0x65 : 0x41; // IDR / P-frame

  // 填充模拟数据
  for (int i = 0; i < 200; i++) {
    buf[offset++] = (uint8_t)(i & 0xFF);
  }

  return offset;
}

/**
 * 生成测试用 PCMA 音频帧 (8kHz, 160 samples = 20ms)
 * 实际设备应从麦克风或编码器获取
 */
static int generate_test_pcma_frame(uint8_t* buf) {
  // PCMA: 8-bit, 8kHz, mono, 160 samples per frame (20ms)
  for (int i = 0; i < 160; i++) {
    buf[i] = 0xD5; // PCMA silence
  }
  return 160;
}

static void print_usage(const char* prog) {
  printf("用法: %s -u <whip_url> [-t <token>] [-a <alaw_file>] [-v <h264_file>]\n", prog);
  printf("\n参数:\n");
  printf("  -u <url>    WHIP 端点 URL (必填)\n");
  printf("              示例: http://192.168.1.100:80/index/api/whip?app=live&stream=camera1\n");
  printf("  -t <token>  Bearer Token (可选)\n");
  printf("  -a <file>   A-law 原始音频 (8kHz mono); 省略则默认推送测试音频\n");
  printf("  -v <file>   H264 Annex-B 视频; 仅指定 -v 时启用视频轨\n");
  printf("              省略 -v 则 SDP/推流均不含视频\n");
  printf("\n示例:\n");
  printf("  %s -u http://192.168.1.100/index/api/whip?app=live&stream=mic -a mic.alaw\n", prog);
  printf("  %s -u http://192.168.1.100/index/api/whip?app=live&stream=cam -v cam.h264 -a mic.alaw\n", prog);
}

static uint64_t get_timestamp_ms(void) {
  struct timeval tv;
  gettimeofday(&tv, NULL);
  return (uint64_t)tv.tv_sec * 1000 + tv.tv_usec / 1000;
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

int main(int argc, char* argv[]) {
  const char* url = NULL;
  const char* token = NULL;
  const char* video_file = NULL;
  const char* audio_file = NULL;
  int enable_video = 0;

  // 解析命令行参数
  for (int i = 1; i < argc; i++) {
    if (strcmp(argv[i], "-u") == 0 && i + 1 < argc) {
      url = argv[++i];
    } else if (strcmp(argv[i], "-t") == 0 && i + 1 < argc) {
      token = argv[++i];
    } else if (strcmp(argv[i], "-v") == 0) {
      enable_video = 1;
      if (i + 1 < argc && argv[i + 1][0] != '-') {
        video_file = argv[++i];
      }
    } else if (strcmp(argv[i], "-a") == 0 && i + 1 < argc) {
      audio_file = argv[++i];
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
  printf(" WHIP 推流示例 (libpeer → ZLMediaKit)\n");
  printf("========================================\n");
  printf(" WHIP URL: %s\n", url);
  printf(" Token:    %s\n", token ? token : "(无)");
  printf(" 视频:     %s\n", enable_video ? (video_file ? video_file : "内置测试数据 (H264)") : "未启用");
  printf(" 音频:     %s\n", audio_file ? audio_file : "内置测试数据 (PCMA)");
  printf("========================================\n\n");

  // 初始化 WebRTC 库
  peer_init();

  // 创建 PeerConnection — 仅启用用户指定的媒体轨
  PeerConfiguration config = {
      .ice_servers = {
          {.urls = "stun:stun.l.google.com:19302"},
      },
      .datachannel = DATA_CHANNEL_NONE,
      .video_codec = enable_video ? CODEC_H264 : CODEC_NONE,
      .audio_codec = CODEC_PCMA,
      .sdp_profile = SDP_PROFILE_WHIP, 
      .skip_stun_check_keepalive = 1, //是否跳过stun check 链接校验; ice-lite模式,通常需要设置为1;
  };

  g_pc = peer_connection_create(&config);
  if (!g_pc) {
    printf("[错误] 创建 PeerConnection 失败\n");
    peer_deinit();
    return 1;
  }

  // 注册连接状态回调
  peer_connection_oniceconnectionstatechange(g_pc, on_connection_state);

  // 通过 HTTP 信令连接 ZLMediaKit WHIP 端点
  // libpeer 内部会:
  //   1. 创建 SDP offer (含 ICE candidates)
  //   2. HTTP POST 到 WHIP URL
  //   3. 解析 201 响应中的 SDP answer
  //   4. 设置 answer 为远程描述
  printf("[WHIP] 正在连接 %s ...\n", url);
  if (peer_signaling_connect(url, token, g_pc) != 0) {
    printf("[错误] 连接 WHIP 端点失败\n");
    peer_connection_destroy(g_pc);
    peer_deinit();
    return 1;
  }

  // 启动事件循环线程 (ICE/DTLS/RTP)
  pthread_t pc_thread;
  pthread_create(&pc_thread, NULL, peer_connection_task, g_pc);

  // 打开音视频文件 (如果指定)
  FILE* fp_video = NULL;
  FILE* fp_audio = NULL;
  if (video_file) {
    fp_video = fopen(video_file, "rb");
    if (!fp_video) {
      printf("[错误] 无法打开视频文件: %s\n", video_file);
      return 1;
    }
  }
  if (audio_file) {
    fp_audio = fopen(audio_file, "rb");
    if (!fp_audio) {
      printf("[错误] 无法打开音频文件: %s\n", audio_file);
      if (fp_video) {
        fclose(fp_video);
      }
      return 1;
    }
  }

  uint64_t video_time = 0;
  uint64_t audio_time = 0;
  int frame_count = 0;
  uint8_t video_buf[4096];
  uint8_t audio_buf[160];
  int keyframe_interval = 0;

  printf("[WHIP] 等待 ICE 连接建立...\n");

  // 主循环: 发送音视频数据
  while (!g_interrupted) {
    if (g_state == PEER_CONNECTION_COMPLETED) {
      uint64_t now = get_timestamp_ms();

      // 视频: 仅 -v 启用时发送
      if (enable_video && now - video_time >= 40) {
        video_time = now;
        int size = 0;

        if (fp_video) {
          // 从文件读取 H264 Annex-B 帧
          // 简化: 每次读 2048 字节 (实际应按 NAL 边界读取)
          size = fread(video_buf, 1, 2048, fp_video);
          if (size <= 0) {
            // 循环播放
            fseek(fp_video, 0, SEEK_SET);
            continue;
          }
        } else {
          // 使用内置测试数据
          keyframe_interval = (keyframe_interval + 1) % 75; // 每 75 帧一个关键帧
          size = generate_test_h264_frame(video_buf, keyframe_interval == 0);
        }

        if (size > 0) {
          peer_connection_send_video(g_pc, video_buf, size);
          frame_count++;
          if (frame_count % 250 == 0) {
            printf("[WHIP] 已发送 %d 帧视频\n", frame_count);
          }
        }
      }

      // 音频: 50 FPS (每 20ms 一帧, 160 samples @ 8kHz)
      if (now - audio_time >= 20) {
        audio_time = now;
        int size = 0;

        if (fp_audio) {
          size = fread(audio_buf, 1, 160, fp_audio);
          if (size <= 0) {
            fseek(fp_audio, 0, SEEK_SET);
            continue;
          }
          if (size < 160) {
            memset(audio_buf + size, 0xD5, 160 - size);
            size = 160;
          }
        } else {
          size = generate_test_pcma_frame(audio_buf);
        }

        if (size > 0) {
          peer_connection_send_audio(g_pc, audio_buf, size);
        }
      }
    }

    usleep(1000);
  }

  printf("\n[WHIP] 正在清理...\n");

  // 清理
  if (fp_video) fclose(fp_video);
  if (fp_audio) fclose(fp_audio);

  peer_signaling_disconnect();
  pthread_join(pc_thread, NULL);
  peer_connection_destroy(g_pc);
  peer_deinit();

  printf("[WHIP] 已退出\n");
  return 0;
}
