/* -*-mode:c++; tab-width: 2; indent-tabs-mode: nil; c-basic-offset: 2 -*- */

/* Copyright 2013-2018 the Alfalfa authors
                       and the Massachusetts Institute of Technology

   Redistribution and use in source and binary forms, with or without
   modification, are permitted provided that the following conditions are
   met:

      1. Redistributions of source code must retain the above copyright
         notice, this list of conditions and the following disclaimer.

      2. Redistributions in binary form must reproduce the above copyright
         notice, this list of conditions and the following disclaimer in the
         documentation and/or other materials provided with the distribution.

   THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
   "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
   LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
   A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
   HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
   SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
   LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
   DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
   THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
   (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
   OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE. */

#include <getopt.h>

#include <cstdlib>
#include <iostream>
#include <chrono>
#include <vector>
#include <random>
#include <limits>
#include <thread>
#include <future>
#include <algorithm>
#include <unordered_map>
#include <iomanip>
#include <cmath>

#include "exception.hh"
#include "finally.hh"
#include "paranoid.hh"
#include "yuv4mpeg.hh"
#include "encoder.hh"
#include "socket.hh"
#include "packet.hh"
#include "poller.hh"
#include "socketpair.hh"
#include "camera.hh"
#include "pacer.hh"
#include "procinfo.hh"

using namespace std;
using namespace std::chrono;
using namespace PollerShortNames;

class AverageEncodingTime
//计算平均编码时间
{
private:
  static constexpr double ALPHA = 0.1;//计算平均编码时间时作为权重因子

  double value_ { -1.0 };//存储当前的平均编码时间
  microseconds last_update_{ 0 };//存储上一次更新平均编码时间的时间戳

public:
  void add( const microseconds timestamp_us )
// 这个函数接收一个时间戳作为参数，然后根据这个时间戳和上一次更新的时间戳来更新平均编码时间。如果新的时间戳比上一次更新的时间戳晚1秒或以上，那么平均编码时间将被重置为0。
// 否则，平均编码时间将被更新为新的编码时间和旧的编码时间的加权平均值，其中新的编码时间的权重为ALPHA，旧的编码时间的权重为1 - ALPHA
  {
    assert( timestamp_us >= last_update_ );
//assert() 是一个宏，用于在调试阶段检查程序的逻辑错误。如果括号内的表达式为真（非零），assert() 不会有任何效果。
//但是，如果表达式为假（零），assert() 将在标准错误上打印一条错误消息，并通过调用 abort() 来终止程序。
    if ( value_ < 0 ) {
      value_ = 0;
    }
    else if ( timestamp_us - last_update_ > 1s /* 1 seconds */ ) {
      value_ = 0;
    }
    else {
      double new_value = max( 0l, duration_cast<microseconds>( timestamp_us - last_update_ ).count() );
      value_ = ALPHA * new_value + ( 1 - ALPHA ) * value_;
    }

    last_update_ = timestamp_us;
  }

  uint32_t int_value() const { return static_cast<uint32_t>( value_ ); }
  // static_cast<uint32_t>( value_ ) 将 value_（一个 double 类型的值）转换为 uint32_t 类型。
  // 这种转换会丢失 value_ 的小数部分，只保留整数部分
};

struct EncodeJob
{
  string name;

  RasterHandle raster;//可能表示需要编码的图像或视频帧

  Encoder encoder;
  EncoderMode mode;

  uint8_t y_ac_qi;//表示量化参数
  size_t target_size;//目标帧大小或目标比特率

  EncodeJob( const string & name, RasterHandle raster, const Encoder & encoder,
             const EncoderMode mode, const uint8_t y_ac_qi, const size_t target_size )//在函数形参列表中，可以加const修饰形参，防止传入的实参值发生改变
    : name( name ), raster( raster ), encoder( encoder ),
      mode( mode ), y_ac_qi( y_ac_qi ), target_size( target_size )
  {}
};

struct EncodeOutput
{
  Encoder encoder;
  vector<uint8_t> frame;
  uint32_t source_minihash;
  milliseconds encode_time;
  string job_name;
  uint8_t y_ac_qi;

  EncodeOutput( Encoder && encoder, vector<uint8_t> && frame,
                const uint32_t source_minihash, const milliseconds encode_time,
                const string & job_name, const uint8_t y_ac_qi )
    : encoder( move( encoder ) ), frame( move( frame ) ),
      source_minihash( source_minihash ), encode_time( encode_time ),
      job_name( job_name ), y_ac_qi( y_ac_qi )
// Encoder && encoder 和 vector<uint8_t> && frame：这两个参数是右值引用，用于接收临时对象或可以被移动的对象。
// 右值引用允许我们在不复制数据的情况下将资源从一个对象转移到另一个对象。
// 使用 std::move 函数将 encoder 和 frame 的资源移动到新创建的 EncodeOutput 对象中，而不是复制这些资源。
// 这可以提高效率，特别是当 encoder 和 frame 包含大量数据时
  {}
};

EncodeOutput do_encode_job( EncodeJob && encode_job )
//&& 表示 encode_job 是一个右值引用。这意味着 encode_job 可以绑定到一个临时的 EncodeJob 对象或者一个可以被移动的 EncodeJob 对象。
//这样，我们就可以在不进行复制的情况下，直接使用这个对象的资源，从而提高代码的效率。
{
  vector<uint8_t> output;

  uint32_t source_minihash = encode_job.encoder.minihash();

  const auto encode_beginning = system_clock::now();

  uint8_t quantizer_in_use = 0;

  switch ( encode_job.mode ) {
  case CONSTANT_QUANTIZER:
    output = encode_job.encoder.encode_with_quantizer( encode_job.raster.get(),
                                                       encode_job.y_ac_qi );
    quantizer_in_use = encode_job.y_ac_qi;
    break;

  case TARGET_FRAME_SIZE:
    output = encode_job.encoder.encode_with_target_size( encode_job.raster.get(),
                                                         encode_job.target_size );
    break;

  default:
    throw runtime_error( "unsupported encoding mode." );
  }

  const auto encode_ending = system_clock::now();
  const auto ms_elapsed = duration_cast<milliseconds>( encode_ending - encode_beginning );

  return { move( encode_job.encoder ), move( output ), source_minihash, ms_elapsed, encode_job.name, quantizer_in_use };//调用构造函数，创建结构体对象
}

size_t target_size( uint32_t avg_delay, const uint64_t last_acked, const uint64_t last_sent,
                    const uint32_t max_delay = 100 * 1000 /* 100 ms = 100,000 us */ )//用于计算目标数据包的大小
// avg_delay（平均延迟）、last_acked（最后确认的序列号）、last_sent（最后发送的序列号）和max_delay（最大延迟，默认值为100,000微秒，即100毫秒）。
{
  if ( avg_delay == 0 ) { avg_delay = 1; }

  /* cerr << "Packets in flight: " << last_sent - last_acked << "\n";计算在飞行中的数据包数量
  cerr << "Avg inter-packet-arrival interval: " << avg_delay << "\n";算平均每个数据包到达的间隔
  cerr << "Imputed delay: " << avg_delay * (last_sent - last_acked) << " us\n"; 计算算推断的延迟*/

  return 1400 * max( 0l, static_cast<int64_t>( max_delay / avg_delay - ( last_sent - last_acked ) ) );
/*这行代码返回的是目标数据包的大小。计算公式为 `1400 * max(0, max_delay / avg_delay - (last_sent - last_acked))`。
解释如下：
- `1400`：这可能是一个数据包的最大字节数。
- `max(0, max_delay / avg_delay - (last_sent - last_acked))`：这部分计算的是可以容忍的最大数据包数量。其中：
    - `max_delay / avg_delay`：这部分表示在最大延迟时间内，理论上可以发送的数据包数量。
    - `(last_sent - last_acked)`：这部分表示当前已发送但未被确认的数据包数量。
    - `max_delay / avg_delay - (last_sent - last_acked)`：这部分表示在最大延迟时间内，还可以发送的数据包数量。如果这个值为负（即当前未确认的数据包数量已经超过了在最大延迟时间内可以发送的数据包数量），则取0。
所以，这行代码的含义是：计算在当前网络状况下，目标数据包的大小，以便进行流量控制。

 这段代码是C++中的一个表达式，它使用了`max`函数和`static_cast`操作符。
 - `max(0l, ...)`：`max`函数返回两个参数中的最大值。在这里，它比较`0l`（表示长整型的0）和另一个表达式的值，返回两者中的最大值。
 - `static_cast<int64_t>(...)`：`static_cast`是C++中的一种类型转换操作符，用于在不同类型之间进行转换。在这里，它将其参数的值转换为`int64_t`类型（64位整型）。
 所以，整个表达式的意思是：将某个值转换为64位整型，然后返回这个值和0之间的最大值。这样可以确保，即使转换的结果是负数，这个表达式也总是返回非负值。*/
}

void usage( const char *argv0 )
{
  cerr << "Usage: " << argv0
       << " [-m,--mode MODE] [-d, --device CAMERA] [-p, --pixfmt PIXEL_FORMAT]"
       << " [-u,--update-rate RATE] [--log-mem-usage] HOST PORT CONNECTION_ID" << endl
       << endl
       << "Accepted MODEs are s1, s2 (default), conventional." << endl;
}

uint64_t ack_seq_no( const AckPacket & ack,
                     const vector<uint64_t> & cumulative_fpf )
{
  return ( ack.frame_no() > 0 )
       ? ( cumulative_fpf[ ack.frame_no() - 1 ] + ack.fragment_no() )
       : ack.fragment_no();
/*
ack可能是指确认包（Acknowledgement Packet）。在网络编程中，当一个数据包从发送端发送到接收端后，接收端通常会发送一个确认包（ACK）回到发送端，以确认它已经成功接收了这个数据包。
在这段代码中，ack是AckPacket类型的一个对象，它可能包含了一些关于确认包的信息，比如确认的帧号（ack.frame_no()）和确认的数据包序号（ack.fragment_no()）。这些信息可以用来更新发送端的状态，比如已确认的数据包数量、未确认的数据包数量等。

`fragment_no`可能是指一个数据包在其所属帧中的序号。在视频编码和网络传输中，一帧视频通常会被分割成多个数据包（或称为片段，fragment）进行发送，每个数据包都有一个在当前帧中的序号，这就是`fragment_no`。
例如，如果一帧被分割成了10个数据包，那么这些数据包的`fragment_no`就会从0（或1）到9。这样，接收端在接收到所有的数据包后，就可以根据`fragment_no`将它们重新组合成完整的帧。

`cumulative_fpf`是一个向量（vector），它存储了每一帧的累积数据包数量。在这个上下文中，`fpf`可能代表“Frame Per Fragment”，即每个数据包中的帧数量。
当一个新的帧被编码并分割成多个数据包时，这些数据包的数量就会被添加到`cumulative_fpf`向量的末尾。因此，`cumulative_fpf[i]`表示的是前`i+1`帧的数据包总数。
这个向量可以用来计算任何数据包在全局范围内的序列号。例如，如果你知道一个数据包是第`i`帧的第`j`个数据包，那么你可以通过`cumulative_fpf[i-1] + j`来计算这个数据包的全局序列号（假设帧和数据包的编号都是从0开始的）。希望这个解释对你有所帮助！

如果ack.frame_no()大于0，那么返回cumulative_fpf[ ack.frame_no() - 1 ] + ack.fragment_no()。这里，cumulative_fpf[ ack.frame_no() - 1 ]获取的是累积的帧内数据包数量，ack.fragment_no()获取的是当前帧内的数据包序号，两者相加得到的是全局的数据包序号。
如果ack.frame_no()不大于0（即为0），那么返回ack.fragment_no()。这表示当前帧是第一帧，因此数据包序号就是当前帧内的数据包序号。
这个函数可能用于网络编程中的可靠传输协议，通过跟踪每个数据包的确认序列号，可以知道哪些数据包已经被接收，哪些数据包可能需要重传。*/
}

enum class OperationMode
{
  S1, S2, Conventional
};

int main( int argc, char *argv[] )
{
  /* check the command-line arguments */
  if ( argc < 1 ) { /* for sticklers */
    abort();//终止执行
  }

  /* camera settings */
  string camera_device = "/dev/video0";
  string pixel_format = "NV12";
  size_t update_rate __attribute__((unused)) = 1;
  OperationMode operation_mode = OperationMode::S2;
  bool log_mem_usage = false;//是否记录内存使用情况

  const option command_line_options[] = {
    { "mode",          required_argument, nullptr, 'm' },
    { "device",        required_argument, nullptr, 'd' },
    { "pixfmt",        required_argument, nullptr, 'p' },
    { "update-rate",   required_argument, nullptr, 'u' },
    { "log-mem-usage", no_argument,       nullptr, 'M' },
    { 0, 0, 0, 0 }
  };

  while ( true ) {//这个循环会一直执行，直到getopt_long函数返回-1，表示所有的命令行参数都已经被解析完。
    const int opt = getopt_long( argc, argv, "d:p:m:u:", command_line_options, nullptr );//getopt_long函数，这是一个用于解析命令行选项的库函数

    if ( opt == -1 ) { break; }

    switch ( opt ) {
    case 'd':
      camera_device = optarg;
      break;

    case 'p':
      pixel_format = optarg;
      break;

    case 'm':
      if ( strcmp( optarg, "s1" ) == 0 ) { operation_mode = OperationMode::S1; }
      else if ( strcmp( optarg, "s2" ) == 0 ) { operation_mode = OperationMode::S2; }
      else if ( strcmp( optarg, "conventional" ) == 0 ) { operation_mode = OperationMode::Conventional; }
      else { throw runtime_error( "unknown operation mode" ); }
      break;

    case 'u':
      update_rate = paranoid::stoul( optarg );
      break;

    case 'M':
      log_mem_usage = true;
      break;

    default:
      usage( argv[ 0 ] );
      return EXIT_FAILURE;
    }
  }

  if ( optind + 2 >= argc ) {//optind是一个全局变量，它在getopt库中被定义。当你使用getopt或getopt_long函数解析命令行参数时，optind会被更新为下一个将被解析的参数的索引。
    usage( argv[ 0 ] );
    return EXIT_FAILURE;
  }

  /* construct Socket for outgoing datagrams */
  UDPSocket socket;
  socket.connect( Address( argv[ optind ], argv[ optind + 1 ] ) );
  socket.set_timestamps();

  /* make pacer to smooth out outgoing packets */
  Pacer pacer;

  /* get connection_id */
  const uint16_t connection_id = paranoid::stoul( argv[ optind + 2 ] );

  /* average inter-packet delay, reported by receiver */
  uint32_t avg_delay = numeric_limits<uint32_t>::max();

  /* keep the number of fragments per frame */
  vector<uint64_t> cumulative_fpf;
  uint64_t last_acked = numeric_limits<uint64_t>::max();

  /* maximum number of frames to be skipped in a row */
  const size_t MAX_SKIPPED = 3;
  size_t skipped_count = 0;

  if ( not PIXEL_FORMAT_STRS.count( pixel_format ) ) {
    throw runtime_error( "unsupported pixel format" );
  }

  /* camera device */
  Camera camera { 1280, 720, PIXEL_FORMAT_STRS.at( pixel_format ), camera_device };

  /* construct the encoder */
  Encoder base_encoder { camera.display_width(), camera.display_height(),
                         false /* two-pass */, REALTIME_QUALITY };

  const uint32_t initial_state = base_encoder.minihash();

  /* encoded frame index */
  unsigned int frame_no = 0;

  /* latest raster that is received from the input */
  Optional<RasterHandle> last_raster;

  /* where we keep the outputs of parallel encoding jobs */
  vector<EncodeJob> encode_jobs;
  vector<future<EncodeOutput>> encode_outputs;

  /* keep the moving average of encoding times */
  AverageEncodingTime avg_encoding_time;

  /* track the last quantizer used */
  uint8_t last_quantizer = 64;

  /* decoder hash => encoder object */
  deque<uint32_t> encoder_states;
  unordered_map<uint32_t, Encoder> encoders { { initial_state, base_encoder } };

  /* latest state of the receiver, based on ack packets */
  Optional<uint32_t> receiver_last_acked_state;
  Optional<uint32_t> receiver_assumed_state;
  deque<uint32_t> receiver_complete_states;

  /* if the receiver goes into an invalid state, for this amount of seconds,
     we will go into a conservative mode: we only encode based on a known state */
  seconds conservative_for { 5 };
  system_clock::time_point conservative_until = system_clock::now();

  /* for 'conventional codec' mode */
  duration<long int, std::nano> cc_update_interval { ( update_rate == 0 ) ? 0 : std::nano::den / update_rate };
  system_clock::time_point next_cc_update = system_clock::now() + cc_update_interval;
  size_t cc_quantizer = 32;
  size_t cc_rate = 0;
  size_t cc_rate_ewma = 0;

  /* :D */
  system_clock::time_point last_sent = system_clock::now();

  /* comment */
  auto encode_start_pipe = UnixDomainSocket::make_pair();
  auto encode_end_pipe = UnixDomainSocket::make_pair();

  /* mem usage timer */
  system_clock::time_point next_mem_usage_report = system_clock::now();

  Poller poller;

  /* fetch frames from webcam */
  poller.add_action( Poller::Action( encode_start_pipe.second, Direction::In,
    [&]() -> Result {
      encode_start_pipe.second.read();

      last_raster = camera.get_next_frame();

      if ( not last_raster.initialized() ) {
        return { ResultType::Exit, EXIT_FAILURE };
      }

      if ( encode_jobs.size() > 0 ) {
        /* a frame is being encoded now */
        return ResultType::Continue;
      }

      /* let's cleanup the stored encoders based on the lastest ack */
      if ( receiver_last_acked_state.initialized() and
           receiver_last_acked_state.get() != initial_state and
           encoders.count( receiver_last_acked_state.get() ) ) {
        // cleaning up
        auto it = encoder_states.begin();

        while ( it != encoder_states.end() ) {
          if ( *it != receiver_last_acked_state.get() and
               *it != receiver_assumed_state.get() ) {
            if ( find( next( it ), encoder_states.end(), *it ) == encoder_states.end() ) {
              encoders.erase( *it );
            }

            it++;
          }
          else {
            break;
          }
        }

        encoder_states.erase( encoder_states.begin(), it );
      }

      RasterHandle raster = last_raster.get();

      uint32_t selected_source_hash = initial_state;

      /* reason about the state of the receiver based on ack messages
       * this is the logic that decides which encoder to use. for example,
       * if the packet loss is huge, we can always select an encoder with a sure
       * state. */

      /* if we're in 'conservative' mode, let's just encode based on something
         we're sure that is available in the receiver */
      if ( system_clock::now() < conservative_until ) {
        if( receiver_complete_states.size() == 0 ) {
          /* and the receiver doesn't have any other states, other than the
             default state */
          selected_source_hash = initial_state;
        }
        else {
          /* the receiver has at least one stored state, let's use it */
          selected_source_hash = receiver_complete_states.back();
        }
      }
      else if ( not receiver_last_acked_state.initialized() ) {
        /* okay, we're not in 'conservative' mode */
        if ( not receiver_assumed_state.initialized() ) {
          /* okay, let's just encode as a keyframe */
          selected_source_hash = initial_state;
        }
        else {
          /* we assume that the receiver is in a right state */
          selected_source_hash = receiver_assumed_state.get();
        }
      }
      else {
        if ( encoders.count( receiver_last_acked_state.get() ) == 0 ) {
          /* it seems that the receiver is in an invalid state */

          /* step 1: let's go into 'conservative' mode; just encode based on a
             known for a while */

          conservative_until = system_clock::now() + conservative_for;

          cerr << "Going into 'conservative' mode for next "
               << conservative_for.count() << " seconds." << endl;

          if( receiver_complete_states.size() == 0 ) {
            /* and the receiver doesn't have any other states, other than the
               default state */
            selected_source_hash = initial_state;
          }
          else {
            /* the receiver has at least one stored state, let's use it */
            selected_source_hash = receiver_complete_states.back();
          }
        }
        else {
          /* we assume that the receiver is in a right state */
          selected_source_hash = receiver_assumed_state.get();
        }
      }
      /* end of encoder selection logic */
      const Encoder & encoder = encoders.at( selected_source_hash );

      const static auto increment_quantizer = []( const uint16_t q, const int8_t inc ) -> uint8_t
        {
          int orig = q;
          orig += inc;
          orig = max( 3, orig );
          orig = min( 127, orig );
          return orig;
        };

      if ( operation_mode == OperationMode::Conventional ) {
        /* is it time to update the quality setting? */
        if ( next_cc_update <= system_clock::now() ) {
          const size_t old_quantizer = cc_quantizer;
          cc_rate = 1000 * 1000 * 1400 / avg_delay;

          double change_percentage = ( 1.0 * cc_rate - 1.0 * cc_rate_ewma ) /
                                     ( 1.0 * cc_rate_ewma );

          change_percentage = max( -1.0, min( 1.5, change_percentage ) );

          if ( change_percentage < -0.99 ) {
            cc_quantizer = 127;
          }
          else {
            double qalpha = 0.75;
            cc_quantizer = increment_quantizer( cc_quantizer /
                                                pow( change_percentage + 1, 1 / qalpha ), 0 );
          }

          cc_rate_ewma = 0.8 * cc_rate + 0.2 * cc_rate_ewma;

          cerr << "avg-delay=" << avg_delay << "us "
               << "old-quantizer=" << old_quantizer << " "
               << "new-quantizer=" << cc_quantizer << " "
               << "emwa-rate=" << cc_rate_ewma / 1000 << "KB "
               << "new-rate=" << cc_rate / 1000 << "KB "
               << "change-percentage="
               << fixed << setprecision( 2 ) << change_percentage
               << endl;

          next_cc_update = system_clock::now() + cc_update_interval;
        }

        encode_jobs.emplace_back( "frame", raster, encoder, CONSTANT_QUANTIZER,
                                  cc_quantizer, 0  );
      }
      else {
        /* try various quantizers */
        encode_jobs.emplace_back( "improve", raster, encoder, CONSTANT_QUANTIZER,
                                  increment_quantizer( last_quantizer, -17 ), 0 );

        encode_jobs.emplace_back( "fail-small", raster, encoder, CONSTANT_QUANTIZER,
                                  increment_quantizer( last_quantizer, +23 ), 0 );
      }

      // this thread will spawn all the encoding jobs and will wait on the results
      thread(
        [&encode_jobs, &encode_outputs, &encode_end_pipe, operation_mode]()
        {
          encode_outputs.clear();
          encode_outputs.reserve( encode_jobs.size() );

          for ( auto & job : encode_jobs ) {
            encode_outputs.push_back(
              async( ( ( operation_mode == OperationMode::S2 ) ? launch::async : launch::deferred ),
                     do_encode_job, move( job ) ) );
          }

          for ( auto & future_res : encode_outputs ) {
            future_res.wait();
          }

          encode_end_pipe.first.write( "1" );
        }
      ).detach();

      return ResultType::Continue;
    } )
  );

  /* all encode jobs have finished */
  poller.add_action( Poller::Action( encode_end_pipe.second, Direction::In,
    [&]()
    {
      /* whatever happens, encode_jobs will be empty after this block is done. */
      auto _ = finally(
        [&]()
        {
          encode_jobs.clear();
          encode_start_pipe.first.write( "1" );
        }
      );

      encode_end_pipe.second.read();

      avg_encoding_time.add( duration_cast<microseconds>( system_clock::now().time_since_epoch() ) );

      if ( not any_of( encode_outputs.cbegin(), encode_outputs.cend(),
                       [&]( const future<EncodeOutput> & o ) { return o.valid(); } ) ) {
        cerr << "All encoding jobs got killed for frame " << frame_no << "\n";
        // no encoding job has ended in time
        return ResultType::Continue;
      }

      /* what is the current capacity of the network,
         now that the encoding is done? */
      size_t frame_size = numeric_limits<size_t>::max();

      if ( avg_delay != numeric_limits<uint32_t>::max() ) {
        frame_size = target_size( avg_delay, last_acked, cumulative_fpf.back() );
      }

      size_t best_output_index = numeric_limits<size_t>::max();
      size_t best_size_diff = numeric_limits<size_t>::max();

      vector<EncodeOutput> good_outputs;

      for ( auto & out_future : encode_outputs ) {
        if ( out_future.valid() ) {
          good_outputs.push_back( move( out_future.get() ) );
        }
      }

      if ( operation_mode == OperationMode::Conventional ) {
        best_output_index = 0; /* always send the frame */
      }
      else {
        /* choose the best based on the current capacity */
        for ( size_t i = 0; i < good_outputs.size(); i++ ) {
          if ( good_outputs[ i ].frame.size() <= frame_size ) {
            if ( frame_size - good_outputs[ i ].frame.size() < best_size_diff ) {
              best_size_diff = frame_size - good_outputs[ i ].frame.size();
              best_output_index = i;
            }
          }
        }

        if ( best_output_index == numeric_limits<size_t>::max() ) {
          if ( skipped_count < MAX_SKIPPED or good_outputs.back().job_name != "fail-small" ) {
            /* skip frame */
            cerr << "["
                 << duration_cast<milliseconds>( system_clock::now().time_since_epoch() ).count()
                 << "] "
                 << "Skipping frame " << frame_no << "\n";
            skipped_count++;
            return ResultType::Continue;
          } else {
            cerr << "Too many skipped frames; sending the bad-quality option on " << frame_no << "\n";
            best_output_index = good_outputs.size() - 1;
            assert( good_outputs[ best_output_index ].job_name == "fail-small" );
          }
        }
      }

      auto output = move( good_outputs[ best_output_index ] );

      uint32_t target_minihash = output.encoder.minihash();

      /*
      cerr << "Sending frame #" << frame_no << " (size=" << output.frame.size() << " bytes, "
           << "source_hash=" << output.source_minihash << ", target_hash="
           << target_minihash << ")...";
      */

      last_quantizer = output.y_ac_qi;

      FragmentedFrame ff { connection_id, output.source_minihash, target_minihash,
                           frame_no,
                           static_cast<uint32_t>( duration_cast<microseconds>( system_clock::now() - last_sent ).count() ),
                           output.frame };
      /* enqueue the packets to be sent */
      /* send 5x faster than packets are being received */
      const unsigned int inter_send_delay = min( 2000u, max( 500u, avg_delay / 5 ) );
      for ( const auto & packet : ff.packets() ) {
        pacer.push( packet.to_string(), inter_send_delay );
      }

      last_sent = system_clock::now();

      /* cerr << "["
           << duration_cast<milliseconds>( last_sent.time_since_epoch() ).count()
           << "] "
           << "Frame " << frame_no << ": " << output.job_name
           << " (" << to_string( output.y_ac_qi ) << ") = "
           << ff.fragments_in_this_frame() << " fragments ("
           << avg_encoding_time.int_value()/1000 << " ms, ssim="
           << output.encoder.stats().ssim.get_or( -1.0 )
           << ") {" << output.source_minihash << " -> " << target_minihash << "}"
           << " intersend_delay = " << inter_send_delay << " us"; */

      if ( log_mem_usage and next_mem_usage_report < last_sent ) {
        cerr << " <mem = " << procinfo::memory_usage() << ">";
        next_mem_usage_report = last_sent + 5s;
      }

      // cerr << "\n";

      cumulative_fpf.push_back( ( frame_no > 0 )
                                ? ( cumulative_fpf[ frame_no - 1 ] + ff.fragments_in_this_frame() )
                                : ff.fragments_in_this_frame() );

      /* now we assume that the receiver will successfully get this */
      receiver_assumed_state.reset( target_minihash );

      encoders.insert( make_pair( target_minihash, move( output.encoder ) ) );
      encoder_states.push_back( target_minihash );

      skipped_count = 0;
      frame_no++;

      return ResultType::Continue;
    } )
  );

  /* new ack from receiver */
  poller.add_action( Poller::Action( socket, Direction::In,
    [&]()
    {
      auto packet = socket.recv();
      AckPacket ack( packet.payload );

      if ( ack.connection_id() != connection_id ) {
        /* this is not an ack for this session! */
        return ResultType::Continue;
      }

      uint64_t this_ack_seq = ack_seq_no( ack, cumulative_fpf );

      if ( last_acked != numeric_limits<uint64_t>::max() and
           this_ack_seq < last_acked ) {
        /* we have already received an ACK newer than this */
        return ResultType::Continue;
      }

      last_acked = this_ack_seq;
      avg_delay = ack.avg_delay();
      receiver_last_acked_state.reset( ack.current_state() );
      receiver_complete_states = move( ack.complete_states() );

      return ResultType::Continue;
    } )
  );

  /* outgoing packet ready to leave the pacer */
  poller.add_action( Poller::Action( socket, Direction::Out, [&]() {
        assert( pacer.ms_until_due() == 0 );

        while ( pacer.ms_until_due() == 0 ) {
          assert( not pacer.empty() );

          socket.send( pacer.front() );
          pacer.pop();
        }

        return ResultType::Continue;
      }, [&]() { return pacer.ms_until_due() == 0; } ) );

  /* kick off the first encode */
  encode_start_pipe.first.write( "1" );

  /* handle events */
  while ( true ) {
    const auto poll_result = poller.poll( pacer.ms_until_due() );
    if ( poll_result.result == Poller::Result::Type::Exit ) {
      if ( poll_result.exit_status ) {
        cerr << "Connection error." << endl;
      }

      return poll_result.exit_status;
    }
  }

  return EXIT_FAILURE;
}
