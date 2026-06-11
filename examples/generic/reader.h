#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/**
 * 初始化媒体读取器
 * 加载 H.264 视频文件和 PCMA 音频文件
 * WebRTC 交互: 准备媒体源，用于后续通过 RTP 发送
 */
int reader_init();

/**
 * 获取一帧 H.264 视频数据
 * WebRTC 交互: 从 H.264 流中提取一帧，用于 RTP 打包发送
 * 返回的缓冲区包含完整的 NALU 数据（含 SPS/PPS 的关键帧或非关键帧）
 * 调用者需要释放返回的缓冲区
 */
uint8_t* reader_get_video_frame(int* size);

/**
 * 获取一帧 PCMA 音频数据
 * WebRTC 交互: 从 PCMA 流中提取 20ms 音频帧，用于 RTP 打包发送
 * 每帧 160 字节（8kHz 采样率，单声道，20ms）
 * 返回的缓冲区指向内部静态缓冲区，不需要释放
 */
uint8_t* reader_get_audio_frame(int* size);

/**
 * 清理资源
 * 释放所有分配的缓冲区
 */
void reader_deinit();
