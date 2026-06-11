#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>

static int g_video_size = 0;
static int g_audio_size = 0;
static uint8_t* g_video_buf = NULL;
static uint8_t* g_audio_buf = NULL;
static uint8_t* g_pps_buf = NULL;
static uint8_t* g_sps_buf = NULL;
static const uint32_t nalu_start_4bytecode = 0x01000000;
static const uint32_t nalu_start_3bytecode = 0x010000;

/**
 * H.264 NALU 类型定义
 * WebRTC 交互: H.264 视频流由多个 NALU（Network Abstraction Layer Unit）组成
 * 不同类型的 NALU 在 WebRTC 中有不同的处理方式:
 *   - SPS/PPS: 序列参数集/图像参数集，解码器初始化必需
 *   - IDR: 关键帧，解码器可以从这里开始解码
 *   - NON_IDR: 非关键帧（P/B 帧），依赖前面的帧解码
 */
typedef enum H264_NALU_TYPE {
  NALU_TYPE_SPS = 7,     // Sequence Parameter Set - 序列参数集
  NALU_TYPE_PPS = 8,     // Picture Parameter Set - 图像参数集
  NALU_TYPE_IDR = 5,     // Instantaneous Decoder Refresh - 关键帧
  NALU_TYPE_NON_IDR = 1, // Non-IDR - 非关键帧（P/B 帧）
} H264_NALU_TYPE;

/**
 * 初始化媒体读取器
 * 加载 H.264 视频文件和 PCMA 音频文件到内存
 */
int reader_init() {
  FILE* video_fp = NULL;
  FILE* audio_fp = NULL;
  char videofile[] = "test.264";
  char audiofile[] = "alaw08m.wav";

  /**
   * 读取 H.264 视频文件
   * WebRTC 交互: H.264 是 WebRTC 最常用的视频编解码器之一
   * 视频文件包含多个 NALU，需要按帧分割后通过 RTP 发送
   */
  video_fp = fopen(videofile, "rb");

  if (video_fp == NULL) {
    printf("open file %s failed\n", videofile);
    return -1;
  }

  fseek(video_fp, 0, SEEK_END);
  g_video_size = ftell(video_fp);
  fseek(video_fp, 0, SEEK_SET);
  g_video_buf = (uint8_t*)calloc(1, g_video_size);
  fread(g_video_buf, g_video_size, 1, video_fp);
  fclose(video_fp);

  /**
   * 读取 PCMA 音频文件
   * WebRTC 交互: PCMA (G.711 A-law) 是 WebRTC 支持的音频编解码器
   * 采样率 8kHz，单声道，每 20ms 一帧（160 采样）
   */
  audio_fp = fopen(audiofile, "rb");

  if (audio_fp == NULL) {
    printf("open file %s failed\n", audiofile);
    return -1;
  }

  fseek(audio_fp, 0, SEEK_END);
  g_audio_size = ftell(audio_fp);
  fseek(audio_fp, 0, SEEK_SET);
  g_audio_buf = (uint8_t*)calloc(1, g_audio_size);
  fread(g_audio_buf, 1, g_audio_size, audio_fp);
  fclose(audio_fp);

  return 0;
}

/**
 * 查找 H.264 NALU 起始码
 * WebRTC 交互: H.264 浏览器流使用起始码（0x000001 或 0x00000001）分隔 NALU
 * 这是解析 H.264 浏览器流的基础操作
 */
uint8_t* reader_h264_find_nalu(uint8_t* buf_start, uint8_t* buf_end) {
  uint8_t* p = buf_start;

  while ((p + 3) < buf_end) {
    if (memcmp(p, &nalu_start_4bytecode, 4) == 0) {
      return p;
    } else if (memcmp(p, &nalu_start_3bytecode, 3) == 0) {
      return p;
    }
    p++;
  }

  return buf_end;
}

/**
 * 获取下一帧 H.264 视频数据
 * WebRTC 交互: 从 H.264 流中提取一帧视频数据用于 RTP 打包
 *
 * 处理逻辑:
 *   1. SPS/PPS: 保存参数集，用于后续关键帧组装
 *   2. IDR (关键帧): 组装 SPS+PPS+IDR 作为完整帧发送
 *      - WebRTC 要求关键帧必须包含 SPS/PPS，否则解码器无法初始化
 *   3. NON_IDR (非关键帧): 直接发送，依赖之前的帧解码
 */
uint8_t* reader_get_video_frame(int* size) {
  uint8_t* buf = NULL;
  static int pps_size = 0;
  static int sps_size = 0;
  uint8_t* buf_end = g_video_buf + g_video_size;

  static uint8_t* pstart = NULL;
  static uint8_t* pend = NULL;
  size_t nalu_size;

  if (!pstart)
    pstart = g_video_buf;

  pend = reader_h264_find_nalu(pstart + 2, buf_end);

  if (pend == buf_end) {
    pstart = NULL;
    return NULL;
  }

  nalu_size = pend - pstart;
  int start_code_offset = memcmp(pstart, &nalu_start_3bytecode, 3) == 0 ? 3 : 4;
  H264_NALU_TYPE nalu_type = (H264_NALU_TYPE)(pstart[start_code_offset] & 0x1f);

  switch (nalu_type) {
    /**
     * SPS (Sequence Parameter Set) - 序列参数集
     * WebRTC 交互: SPS 包含视频序列的全局参数（分辨率、帧率等）
     * 解码器需要 SPS 才能初始化，通常在关键帧前发送
     */
    case NALU_TYPE_SPS:
      sps_size = nalu_size;
      if (g_sps_buf != NULL) {
        free(g_sps_buf);
        g_sps_buf = NULL;
      }
      g_sps_buf = (uint8_t*)calloc(1, sps_size);
      memcpy(g_sps_buf, pstart, sps_size);
      break;

    /**
     * PPS (Picture Parameter Set) - 图像参数集
     * WebRTC 交互: PPS 包含图像编码参数（量化参数、熵编码模式等）
     * 解码器需要 PPS 才能解码图像，通常在关键帧前发送
     */
    case NALU_TYPE_PPS:
      pps_size = nalu_size;
      if (g_pps_buf != NULL) {
        free(g_pps_buf);
        g_pps_buf = NULL;
      }
      g_pps_buf = (uint8_t*)calloc(1, pps_size);
      memcpy(g_pps_buf, pstart, pps_size);

      break;

    /**
     * IDR (Instantaneous Decoder Refresh) - 关键帧
     * WebRTC 交互: IDR 帧是完整的图像帧，解码器可以从这里开始解码
     * 在 WebRTC 中，关键帧通常在以下情况发送:
     *   - 连接建立初期（确保解码器能正确初始化）
     * - 网络丢包后请求关键帧恢复
     *   - 分辨率/帧率变化时
     *
     * 关键帧必须包含 SPS+PPS+IDR，否则接收端无法解码
     */
    case NALU_TYPE_IDR:
      *size = sps_size + pps_size + nalu_size;
      buf = (uint8_t*)calloc(1, *size);
      memcpy(buf, g_sps_buf, sps_size);
      memcpy(buf + sps_size, g_pps_buf, pps_size);
      memcpy(buf + sps_size + pps_size, pstart, nalu_size);

      break;

    /**
     * NON_IDR (非关键帧) - P/B 帧
     * WebRTC 交互: 非关键帧依赖前面的帧进行解码
     * 优点: 体积小，压缩率高
     * 缺点: 丢失后无法独立解码，需要等待下一个关键帧
     */
    case NALU_TYPE_NON_IDR:
    default:
      *size = nalu_size;
      buf = (uint8_t*)calloc(1, *size);
      memcpy(buf, pstart, nalu_size);

      break;
  }

  pstart = pend;

  return buf;
}

/**
 * 获取一帧 PCMA 音频数据
 * WebRTC 交互: 从 PCMA 音频流中提取 20ms 音频帧用于 RTP 打包
 *
 * PCMA (G.711 A-law) 参数:
 *   - 采样率: 8000 Hz
 *   - 声道数: 1 (单声道)
 *   - 每帧时长: 20ms
 *   - 每帧采样数: 160 (8000 * 0.02)
 *   - 每帧字节数: 160 (每个采样 8-bit)
 */
uint8_t* reader_get_audio_frame(int* size) {
  uint8_t* buf = NULL;
  static int pos = 0;
  *size = 160;
  if ((pos + *size) > g_audio_size) {
    pos = 0;
  }

  buf = g_audio_buf + pos;
  pos += *size;

  return buf;
}

/**
 * 清理资源
 * 释放所有分配的缓冲区
 */
void reader_deinit() {
  if (g_sps_buf != NULL) {
    free(g_sps_buf);
    g_sps_buf = NULL;
  }

  if (g_pps_buf != NULL) {
    free(g_pps_buf);
    g_pps_buf = NULL;
  }

  if (g_video_buf != NULL) {
    free(g_video_buf);
    g_video_buf = NULL;
  }

  if (g_audio_buf != NULL) {
    free(g_audio_buf);
    g_audio_buf = NULL;
  }
}
