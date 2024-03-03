package ffmpeg_go

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/laoflch/goav/avcodec"
	"github.com/laoflch/goav/avformat"
	"github.com/laoflch/goav/avutil"
)

// Input file URL (ffmpeg ``-i`` option)
//
// Any supplied kwargs are passed to ffmpeg verbatim (e.g. ``t=20``,
// ``f='mp4'``, ``acodec='pcm'``, etc.).
//
// To tell ffmpeg to read from stdin, use ``pipe:`` as the filename.
//
// Official documentation: `Main options <https://ffmpeg.org/ffmpeg.html#Main-options>`__
func Input(filename string, kwargs ...KwArgs) *Stream {
	args := MergeKwArgs(kwargs)
	args["filename"] = filename
	if fmt := args.PopString("f"); fmt != "" {
		if args.HasKey("format") {
			panic(errors.New("can't specify both `format` and `f` options"))
		}
		args["format"] = fmt
	}
	return NewInputNode("input", nil, args).Stream("", "")
}

// Add extra global command-line argument(s), e.g. ``-progress``.
func (s *Stream) GlobalArgs(args ...string) *Stream {
	if s.Type != "OutputStream" {
		panic("cannot overwrite outputs on non-OutputStream")
	}
	return NewGlobalNode("global_args", []*Stream{s}, args, nil).Stream("", "")
}

// Overwrite output files without asking (ffmpeg ``-y`` option)
//
// Official documentation: `Main options <https://ffmpeg.org/ffmpeg.html#Main-options>`_
func (s *Stream) OverwriteOutput(stream *Stream) *Stream {
	if s.Type != "OutputStream" {
		panic("cannot overwrite outputs on non-OutputStream")
	}
	return NewGlobalNode("overwrite_output", []*Stream{stream}, []string{"-y"}, nil).Stream("", "")
}

// Include all given outputs in one ffmpeg command line
func MergeOutputs(streams ...*Stream) *Stream {
	return NewMergeOutputsNode("merge_output", streams).Stream("", "")
}

//Output file URL
//
//    Syntax:
//        `ffmpeg.Output([]*Stream{stream1, stream2, stream3...}, filename, kwargs)`
//
//    Any supplied keyword arguments are passed to ffmpeg verbatim (e.g.
//    ``t=20``, ``f='mp4'``, ``acodec='pcm'``, ``vcodec='rawvideo'``,
//    etc.).  Some keyword-arguments are handled specially, as shown below.
//
//    Args:
//        video_bitrate: parameter for ``-b:v``, e.g. ``video_bitrate=1000``.
//        audio_bitrate: parameter for ``-b:a``, e.g. ``audio_bitrate=200``.
//        format: alias for ``-f`` parameter, e.g. ``format='mp4'``
//            (equivalent to ``f='mp4'``).
//
//    If multiple streams are provided, they are mapped to the same
//    output.
//
//    To tell ffmpeg to write to stdout, use ``pipe:`` as the filename.
//
//    Official documentation: `Synopsis <https://ffmpeg.org/ffmpeg.html#Synopsis>`__
//    """
func Output(streams []*Stream, fileName string, kwargs ...KwArgs) *Stream {
	args := MergeKwArgs(kwargs)
	if !args.HasKey("filename") {
		if fileName == "" {
			panic("filename must be provided")
		}
		args["filename"] = fileName
	}

	return NewOutputNode("output", streams, nil, args).Stream("", "")
}

func (s *Stream) Output(fileName string, kwargs ...KwArgs) *Stream {
	if s.Type != "FilterableStream" {
		log.Panic("cannot output on non-FilterableStream")
	}
	if strings.HasPrefix(fileName, "s3://") {
		return s.outputS3Stream(fileName, kwargs...)
	}
	return Output([]*Stream{s}, fileName, kwargs...)
}

func (s *Stream) outputS3Stream(fileName string, kwargs ...KwArgs) *Stream {
	r, w := io.Pipe()
	fileL := strings.SplitN(strings.TrimPrefix(fileName, "s3://"), "/", 2)
	if len(fileL) != 2 {
		log.Panic("s3 file format not valid")
	}
	args := MergeKwArgs(kwargs)
	awsConfig := args.PopDefault("aws_config", &aws.Config{}).(*aws.Config)
	bucket, key := fileL[0], fileL[1]
	o := Output([]*Stream{s}, "pipe:", args).
		WithOutput(w, os.Stdout)
	done := make(chan struct{})
	runHook := RunHook{
		f: func() {
			defer func() {
				done <- struct{}{}
			}()

			sess, err := session.NewSession(awsConfig)
			uploader := s3manager.NewUploader(sess)
			_, err = uploader.Upload(&s3manager.UploadInput{
				Bucket: &bucket,
				Key:    &key,
				Body:   r,
			})
			//fmt.Println(ioutil.ReadAll(r))
			if err != nil {
				log.Println("upload fail", err)
			}
		},
		done:   done,
		closer: w,
	}
	o.Context = context.WithValue(o.Context, "run_hook", &runHook)
	return o
}

func PushStreamRtspNative(file_name string, out_url string) error {

	if strings.Trim(file_name, " ") == "" || strings.Trim(out_url, " ") == "" {

		return errors.New(" push stream to rtsp: filename or out_url is empty")

	}

	var ifmt_ctx, ofmt_ctx *avformat.Context
	//var ofmt_ctx **avformat.Context

	//ofmt_ctx_pointer := &avformat.Context{}
	//ofmt_ctx = &ofmt_ctx_pointer
	//ofmt_ctx = &avformat.Context{}

	defer func() {

		ifmt_ctx.AvformatCloseInput()

		if ofmt_ctx != nil && (ofmt_ctx.Oformat().Flags()&avformat.AVFMT_NOFILE) > 0 {
			ofmt_ctx.Pb().CloseP()
			ofmt_ctx.AvformatFreeContext()
		}

	}()

	avutil.AvLogSetLevel(avutil.AV_LOG_DEBUG)

	if avformat.AvformatOpenInput(&ifmt_ctx, file_name, nil, nil) < 0 {

		msg := fmt.Sprintf("Could not open input file '%s'", file_name)

		return errors.New(msg)
	}

	if ifmt_ctx.AvformatFindStreamInfo(nil) < 0 {

		msg := fmt.Sprintf("Failed to retrieve input stream information")

		return errors.New(msg)

	}

	ifmt_ctx.AvDumpFormat(0, file_name, 0)

	//ofmt_ctx := avformat.Context{}
	if avformat.AvformatAllocOutputContext2(&ofmt_ctx, nil, "RTSP", out_url) < 0 {
		msg := fmt.Sprintf("Could not open output rtsp path '%s'", file_name)

		return errors.New(msg)

	} else {
		if ofmt_ctx == nil {
			msg := fmt.Sprintf("Could not create output context")

			return errors.New(msg)

		}
	}

	avutil.AvOptSet(ofmt_ctx.PrivData(), "rtsp_transport", "tcp", 0)

	ofmt_ctx.SetMaxInterleaveDelta(1000000)

	stream_mapping_size := ifmt_ctx.NbStreams()

	stream_mapping := make([]int, stream_mapping_size)

	stream_index := 0

	ofmt := ofmt_ctx.Oformat()

	pkt := avcodec.Packet{}

	//pkt := avcodec.AvPacketAlloc()

	for i := 0; i < int(ifmt_ctx.NbStreams()); i++ {

		in_stream := ifmt_ctx.Streams()[i]
		in_codecpar := in_stream.CodecParameters()

		if in_codecpar.CodecType() != avformat.AVMEDIA_TYPE_VIDEO && in_codecpar.CodecType() != avformat.AVMEDIA_TYPE_AUDIO && in_codecpar.CodecType() != avformat.AVMEDIA_TYPE_SUBTITLE {
			stream_mapping[i] = -1

			continue
		}

		stream_mapping[i] = stream_index
		stream_index++
		fmt.Println("stream_index:", stream_index)
		out_stream := ofmt_ctx.AvformatNewStream(nil)
		if out_stream == nil {
			msg := fmt.Sprintf("Failed allocation ouput steam")

			return errors.New(msg)

		}

		if avcodec.AvCodecParametersCopy(out_stream.CodecParameters(), in_codecpar) < 0 {
			msg := fmt.Sprintf("Failed to copy codec parametrs")

			return errors.New(msg)

		}

		out_stream.CodecParameters().SetCodecTag(0)
	}

	ofmt_ctx.AvDumpFormat(0, out_url, 1)

	if (ofmt.Flags() & avformat.AVFMT_NOFILE) == 0 {

		if pb, err := avformat.AvIOOpen(out_url, avformat.AVIO_FLAG_WRITE); err != nil {
			msg := fmt.Sprintf("Conuld not open output file '%s'", out_url)

			return errors.New(msg)

		} else {
			ofmt_ctx.SetPb(pb)
		}
	}

	if ofmt_ctx.AvformatWriteHeader(nil) < 0 {
		msg := fmt.Sprintf("Error occurred when openging output file '%s'", out_url)

		return errors.New(msg)

	}
	fmt.Println(pkt)
	for {

		if ret := ifmt_ctx.AvReadFrame(&pkt); ret < 0 {

			if ret != avutil.AvErrorEOF {
				msg := fmt.Sprintf("Error occureed: '%s'", avutil.ErrorFromCode(ret))

				return errors.New(msg)

			}
			break
		}
		fmt.Println(pkt)
		in_stream := ifmt_ctx.Streams()[pkt.StreamIndex()]

		if pkt.StreamIndex() >= int(stream_mapping_size) || stream_mapping[pkt.StreamIndex()] < 0 {

			pkt.AvPacketUnref()
			continue
		}

		pkt.SetStreamIndex(stream_mapping[pkt.StreamIndex()])

		out_stream := ofmt_ctx.Streams()[pkt.StreamIndex()]

		logPacket(ifmt_ctx, &pkt, "in")
		//fmt.Printf("%d,%d,%d,%d,%d \n", pkt.Pts(), pkt.Dts(), avutil.AvRescaleQRnd(pkt.Pts(), avutil.NewRational(in_stream.TimeBase().Num(), in_stream.TimeBase().Den()), avutil.NewRational(out_stream.TimeBase().Num(), out_stream.TimeBase().Den()), avutil.AV_ROUND_NEAR_INF|avutil.AV_ROUND_PASS_MINMAX), avutil.AvRescaleQRnd(pkt.Dts(), avutil.NewRational(in_stream.TimeBase().Num(), in_stream.TimeBase().Den()), avutil.NewRational(out_stream.TimeBase().Num(), out_stream.TimeBase().Den()), avutil.AV_ROUND_NEAR_INF|avutil.AV_ROUND_PASS_MINMAX), avutil.AvRescaleQ(int64(pkt.Duration()), avutil.NewRational(in_stream.TimeBase().Num(), in_stream.TimeBase().Den()), avutil.NewRational(out_stream.TimeBase().Num(), out_stream.TimeBase().Den())))
		pkt.SetPts(avutil.AvRescaleQRnd(pkt.Pts(), avutil.NewRational(in_stream.TimeBase().Num(), in_stream.TimeBase().Den()), avutil.NewRational(out_stream.TimeBase().Num(), out_stream.TimeBase().Den()), avutil.AV_ROUND_NEAR_INF|avutil.AV_ROUND_PASS_MINMAX))
		pkt.SetDts(avutil.AvRescaleQRnd(pkt.Dts(), avutil.NewRational(in_stream.TimeBase().Num(), in_stream.TimeBase().Den()), avutil.NewRational(out_stream.TimeBase().Num(), out_stream.TimeBase().Den()), avutil.AV_ROUND_NEAR_INF|avutil.AV_ROUND_PASS_MINMAX))
		pkt.SetDuration(avutil.AvRescaleQ(int64(pkt.Duration()), avutil.NewRational(in_stream.TimeBase().Num(), in_stream.TimeBase().Den()), avutil.NewRational(out_stream.TimeBase().Num(), out_stream.TimeBase().Den())))
		//fmt.Println(pkt)

		pkt.SetPos(-1)

		logPacket(ofmt_ctx, &pkt, "in")

		if (*ofmt_ctx).AvInterleavedWriteFrame(&pkt) < 0 {

			msg := fmt.Sprintf("Error muxing packet")

			return errors.New(msg)
		}

		pkt.AvPacketUnref()
	}

	ofmt_ctx.AvWriteTrailer()

	return nil

}

func logPacket(fmt_ctx *avformat.Context, pkt *avcodec.Packet, tag string) {
	//time_base := fmt_ctx.Streams()[pkt.StreamIndex()].TimeBase()

	//fmt.Sprintf("%s: pts:%s pts_time:%s dts:%s dts_time:%s duration:%s duration_time:%s stream_index:%d", tag, avutil.AvTs2str(pkt.Pts()), avutil.AvTs2timestr(pkt.Pts(), time_base), tag, avutil.AvTs2str(pkt.Dts()), avutil.AvTs2timestr(pkt.Dts(), time_base), avutil.AvTs2str(pkt.Duration()), avutil.AvTs2timestr(pkt.Duration(), time_base))
}
