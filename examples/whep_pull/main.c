/**
 * @file main.c
 * @brief WHEP 拉流示例 — 从 ZLMediaKit 通过 WebRTC 拉取已有媒体流
 *
 * 流程:
 *   1. 创建 PeerConnection (recvonly SDP, SDP_PROFILE_WHEP)
 *   2. HTTP POST SDP offer 到 WHEP 端点
 *   3. 接收 201/200 响应中的 SDP answer
 *   4. ICE → DTLS → 在 onaudiotrack/onvideotrack 回调中接收 RTP 负载
 *
 * 用法:
 *   ./whep_pull -u http://host/index/api/whep?app=live&stream=mic
 *   ./whep_pull -u http://host/index/api/webrtc?app=live&stream=mic&type=play -o out.alaw
 *   ./whep_pull -u http://host/index/api/whep?app=live&stream=cam -v -o out.alaw -O out.h264
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

static FILE* g_fp_audio = NULL;
static FILE* g_fp_video = NULL;
static uint64_t g_audio_bytes = 0;
static uint64_t g_video_bytes = 0;
static uint64_t g_audio_frames = 0;
static uint64_t g_video_frames = 0;

static void signal_handler(int sig) {
  g_interrupted = 1;
}

static void on_connection_state(PeerConnectionState state, void* data) {
  printf("[WHEP] 连接状态: %s\n", peer_connection_state_to_string(state));
  g_state = state;
}

static void on_audio_track(uint8_t* data, size_t size, void* userdata) {
  g_audio_frames++;
  g_audio_bytes += size;

  if (g_fp_audio) {
    fwrite(data, 1, size, g_fp_audio);
  }

  if (g_audio_frames % 250 == 0) {
    printf("[WHEP] 已接收音频 %llu 帧, %llu 字节\n",
           (unsigned long long)g_audio_frames, (unsigned long long)g_audio_bytes);
  }
}

static void on_video_track(uint8_t* data, size_t size, void* userdata) {
  g_video_frames++;
  g_video_bytes += size;

  if (g_fp_video) {
    fwrite(data, 1, size, g_fp_video);
  }

  if (g_video_frames % 100 == 0) {
    printf("[WHEP] 已接收视频 %llu 帧, %llu 字节\n",
           (unsigned long long)g_video_frames, (unsigned long long)g_video_bytes);
  }
}

static void print_usage(const char* prog) {
  printf("用法: %s -u <whep_url> [-t <token>] [-o <alaw_out>] [-O <h264_out>] [-v] [--audio-ssrc <ssrc>] [--video-ssrc <ssrc>]\n", prog);
  printf("\n参数:\n");
  printf("  -u <url>    WHEP 端点 URL (必填)\n");
  printf("              示例: http://host/index/api/whep?app=live&stream=mic\n");
  printf("              兼容: http://host/index/api/webrtc?app=live&stream=mic&type=play\n");
  printf("  -t <token>  Bearer Token (可选)\n");
  printf("  -o <file>   将收到的 PCMA 音频写入 A-law 原始文件 (可选)\n");
  printf("  -O <file>   将收到的 H264 Annex-B 写入文件 (需 -v)\n");
  printf("  -v          SDP 中声明接收视频轨\n");
  printf("  --audio-ssrc <ssrc>  指定本地音频 RTCP SSRC; 省略或 0 时自动随机生成\n");
  printf("  --video-ssrc <ssrc>  指定本地视频 RTCP SSRC; 省略或 0 时自动随机生成\n");
}

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
  const char* audio_out = NULL;
  const char* video_out = NULL;
  uint32_t audio_ssrc = 0;
  uint32_t video_ssrc = 0;
  int enable_video = 0;

  for (int i = 1; i < argc; i++) {
    if (strcmp(argv[i], "-u") == 0 && i + 1 < argc) {
      url = argv[++i];
    } else if (strcmp(argv[i], "-t") == 0 && i + 1 < argc) {
      token = argv[++i];
    } else if (strcmp(argv[i], "-o") == 0 && i + 1 < argc) {
      audio_out = argv[++i];
    } else if (strcmp(argv[i], "-O") == 0 && i + 1 < argc) {
      video_out = argv[++i];
    } else if (strcmp(argv[i], "-v") == 0) {
      enable_video = 1;
    } else if (strcmp(argv[i], "--audio-ssrc") == 0 && i + 1 < argc) {
      audio_ssrc = (uint32_t)strtoul(argv[++i], NULL, 10);
    } else if (strcmp(argv[i], "--video-ssrc") == 0 && i + 1 < argc) {
      video_ssrc = (uint32_t)strtoul(argv[++i], NULL, 10);
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
  printf(" WHEP 拉流示例 (ZLMediaKit → libpeer)\n");
  printf("========================================\n");
  printf(" WHEP URL: %s\n", url);
  printf(" Token:    %s\n", token ? token : "(无)");
  printf(" 音频输出: %s\n", audio_out ? audio_out : "(仅统计, 不写文件)");
  printf(" 视频:     %s\n", enable_video ? (video_out ? video_out : "接收 (不写文件)") : "未启用");
  printf(" Audio SSRC: %u%s\n", audio_ssrc, audio_ssrc ? "" : " (auto)");
  printf(" Video SSRC: %u%s\n", video_ssrc, video_ssrc ? "" : " (auto)");
  printf("========================================\n\n");

  if (audio_out) {
    g_fp_audio = fopen(audio_out, "wb");
    if (!g_fp_audio) {
      printf("[错误] 无法创建音频输出文件: %s\n", audio_out);
      return 1;
    }
  }

  if (enable_video && video_out) {
    g_fp_video = fopen(video_out, "wb");
    if (!g_fp_video) {
      printf("[错误] 无法创建视频输出文件: %s\n", video_out);
      if (g_fp_audio) {
        fclose(g_fp_audio);
      }
      return 1;
    }
  }

  peer_init();

  PeerConfiguration config = {
      .ice_servers = {
          {.urls = "stun:www.icrad.ltd:3478"},
      },
      .datachannel = DATA_CHANNEL_NONE,
      .video_codec = enable_video ? CODEC_H264 : CODEC_NONE,
      .audio_codec = CODEC_PCMA,
      .sdp_profile = SDP_PROFILE_WHEP,
      .skip_stun_check_keepalive = 1,
      .local_audio_ssrc = audio_ssrc,
      .local_video_ssrc = video_ssrc,
      .onaudiotrack = on_audio_track,
      .onvideotrack = enable_video ? on_video_track : NULL,
  };

  g_pc = peer_connection_create(&config);
  if (!g_pc) {
    printf("[错误] 创建 PeerConnection 失败\n");
    peer_deinit();
    return 1;
  }

  peer_connection_oniceconnectionstatechange(g_pc, on_connection_state);

  printf("[WHEP] 正在连接 %s ...\n", url);
  if (peer_signaling_connect(url, token, g_pc) != 0) {
    printf("[错误] 连接 WHEP 端点失败\n");
    peer_connection_destroy(g_pc);
    peer_deinit();
    return 1;
  }

  pthread_t pc_thread;
  pthread_create(&pc_thread, NULL, peer_connection_task, g_pc);

  printf("[WHEP] 等待 ICE 连接并接收媒体 (Ctrl+C 退出)...\n");

  while (!g_interrupted) {
    usleep(100000);
  }

  printf("\n[WHEP] 正在清理...\n");
  printf("[WHEP] 总计: 音频 %llu 帧 / %llu 字节, 视频 %llu 帧 / %llu 字节\n",
         (unsigned long long)g_audio_frames, (unsigned long long)g_audio_bytes,
         (unsigned long long)g_video_frames, (unsigned long long)g_video_bytes);

  peer_signaling_disconnect();
  pthread_join(pc_thread, NULL);
  peer_connection_destroy(g_pc);
  peer_deinit();

  if (g_fp_audio) {
    fclose(g_fp_audio);
  }
  if (g_fp_video) {
    fclose(g_fp_video);
  }

  printf("[WHEP] 已退出\n");
  return 0;
}
