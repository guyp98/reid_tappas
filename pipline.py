import gi
gi.require_version('Gst', '1.0')
from gi.repository import Gst
import threading

Gst.init(None)

# Paths to files
cropper1_lib = "/local/workspace/tappas/apps/h8/gstreamer/libs/post_processes//cropping_algorithms/libwhole_buffer.so"
yolov5_hef = "/local/workspace/tappas/apps/h8/gstreamer/general/vaan/11_03_2024/hailo_example/resources/yolov5s_personface_reid.hef"
yolov5_config = "/local/workspace/tappas/apps/h8/gstreamer/general/vaan/11_03_2024/hailo_example/resources/configs/yolov5_personface.json"
yolo_post_lib = "/local/workspace/tappas/apps/h8/gstreamer/libs/post_processes//libyolo_post.so"
reid_hef = "/local/workspace/tappas/apps/h8/gstreamer/general/vaan/11_03_2024/hailo_example/resources/repvgg_a0_person_reid_2048.hef"
cropping_algo = "/local/workspace/tappas/apps/h8/gstreamer/libs/post_processes//cropping_algorithms/libre_id.so"
reid_lib = "/local/workspace/tappas/apps/h8/gstreamer/libs/post_processes//libre_id.so"
overlay_lib = "/local/workspace/tappas/apps/h8/gstreamer/libs/apps/re_id//libre_id_overlay.so"

#number of files determines the number of streams (no more then 4 streams are supported for now)
source_files = [ 
    "/local/workspace/tappas/apps/h8/gstreamer/general/vaan/11_03_2024/hailo_example/resources/reid0.mp4",
    "/local/workspace/tappas/apps/h8/gstreamer/general/vaan/11_03_2024/hailo_example/resources/reid1.mp4",
    "/local/workspace/tappas/apps/h8/gstreamer/general/vaan/11_03_2024/hailo_example/resources/reid2.mp4",
    "/local/workspace/tappas/apps/h8/gstreamer/general/vaan/11_03_2024/hailo_example/resources/reid3.mp4"
]
hailostreamrouter_name = "sid"
compositor_name = "comp"
hailoroundrobin_name = "fun"

def build_src_streams(hailoroundrobin_name: str, source_files: list):
    source_streams = ""
    for stream_num, source_file in enumerate(source_files):
        stream = \
f"""filesrc location={source_file} name=source_{stream_num} \
! decodebin \
! queue name=hailo_preprocess_q_{stream_num} leaky=no max_size_buffers=30 max-size-bytes=0 max-size-time=0 \
! {hailoroundrobin_name}.sink_{stream_num} 
"""
        source_streams += stream
    return source_streams

def connect_router_and_compositor(hailostreamrouter_name: str, compositor_name:str, num_streams: int):
    conections = ""
    for stream_num in range(num_streams):
        connection = \
f""" {hailostreamrouter_name}.src_{stream_num} 
! queue name=comp_q_{stream_num} leaky=no max-size-buffers=30 max-size-bytes=0 max-size-time=0 
! {compositor_name}.sink_{stream_num} 
"""
        conections += connection
    return conections

def match_stream_to_metadata(num_streams: int):
    metadata = ""
    for stream_num in range(num_streams):
        metadata += f""" src_{stream_num}::input-streams="<sink_{stream_num}>" """
    return metadata




pipeline_desc = f"""
hailoroundrobin mode=1 name={hailoroundrobin_name} 
! queue name=hailo_pre_convert_0 leaky=no max-size-buffers=30 max-size-bytes=0 max-size-time=0 
! videoconvert n-threads=1 qos=false 
! video/x-raw,format=RGB 
! queue name=hailo_pre_cropper1_q leaky=no max-size-buffers=30 max-size-bytes=0 max-size-time=0 
! hailocropper so-path={cropper1_lib} function-name=create_crops use-letterbox=true resize-method=inter-area internal-offset=true name=cropper1 hailoaggregator name=agg1 cropper1. 
! queue name=bypess1_q leaky=no max-size-buffers=30 max-size-bytes=0 max-size-time=0 
! agg1. cropper1. 
! queue name=hailo_pre_detector_q leaky=no max-size-buffers=30 max-size-bytes=0 max-size-time=0 
! hailonet hef-path={yolov5_hef} scheduling-algorithm=1 vdevice-key=1 
! queue name=detector_post_q leaky=no max-size-buffers=1000 max-size-bytes=0 max-size-time=0 
! hailofilter so-path={yolo_post_lib} qos=false function_name=yolov5_personface_letterbox config-path={yolov5_config} 
! queue name=detector_pre_agg_q leaky=no max-size-buffers=30 max-size-bytes=0 max-size-time=0 
! agg1. agg1. 
! queue name=hailo_pre_tracker leaky=no max-size-buffers=30 max-size-bytes=0 max-size-time=0 
! hailotracker name=hailo_tracker hailo-objects-blacklist=hailo_landmarks,hailo_depth_mask,hailo_class_mask,hailo_matrix class-id=1 kalman-dist-thr=0.7 iou-thr=0.7 init-iou-thr=0.8 keep-new-frames=2 keep-tracked-frames=4 keep-lost-frames=8 qos=false std-weight-position-box=0.01 std-weight-velocity-box=0.001 
! queue name=hailo_pre_cropper2_q leaky=no max-size-buffers=30 max-size-bytes=0 max-size-time=0 
! hailocropper so-path={cropping_algo} function-name=create_crops internal-offset=true name=cropper2 hailoaggregator name=agg2 cropper2. 
! queue name=bypess2_q leaky=no max-size-buffers=30 max-size-bytes=0 max-size-time=0 
! agg2. cropper2. 
! queue name=pre_reid_q leaky=no max-size-buffers=10 max-size-bytes=0 max-size-time=0 
! hailonet hef-path={reid_hef} scheduling-algorithm=1 vdevice-key=1 
! queue name=reid_post_q leaky=no max-size-buffers=10 max-size-bytes=0 max-size-time=0 
! hailofilter so-path={reid_lib} qos=false 
! queue name=reid_pre_agg_q leaky=no max-size-buffers=10 max-size-bytes=0 max-size-time=0 
! agg2. agg2. 
! queue name=hailo_pre_gallery leaky=no max-size-buffers=30 max-size-bytes=0 max-size-time=0 
! hailogallery similarity-thr=.4 gallery-queue-size=100 class-id=1 
! queue name=hailo_post_gallery leaky=no max-size-buffers=30 max-size-bytes=0 max-size-time=0 
! videoscale n-threads=2 add-borders=false qos=false 
! video/x-raw, width=800, height=450, pixel-aspect-ratio=1/1 
! queue name=hailo_pre_draw leaky=no max-size-buffers=30 max-size-bytes=0 max-size-time=0 
! hailofilter use-gst-buffer=true so-path={overlay_lib} qos=false 
! queue name=hailo_post_draw leaky=no max-size-buffers=30 max-size-bytes=0 max-size-time=0 
! hailostreamrouter name={hailostreamrouter_name} {match_stream_to_metadata(len(source_files))} 
compositor name={compositor_name} start-time-selection=0 sink_0::xpos=0 sink_0::ypos=0 sink_1::xpos=800 sink_1::ypos=0 sink_2::xpos=0 sink_2::ypos=450 sink_3::xpos=800 sink_3::ypos=450 
! queue name=hailo_video_q_0 leaky=no max_size_buffers=30 max-size-bytes=0 max-size-time=0 
! videoconvert n-threads=2 qos=false 
! queue name=hailo_display_q_0 leaky=no max_size_buffers=300 max-size-bytes=0 max-size-time=0 
! fpsdisplaysink video-sink=ximagesink text-overlay=false name=hailo_display sync=false 
{connect_router_and_compositor(hailostreamrouter_name, compositor_name, len(source_files))} {build_src_streams(hailoroundrobin_name, source_files)}
"""

pipeline = Gst.parse_launch(pipeline_desc)

pipeline.set_state(Gst.State.PLAYING)

def on_eos(bus, msg):
    print("End of Stream Reached")

def on_error(bus, msg):
    error = msg.parse_error()
    print(f"Error: {error[1]}")


bus = pipeline.get_bus()


def message_handler(bus):
    msg = bus.timed_pop_filtered(Gst.CLOCK_TIME_NONE, Gst.MessageType.ERROR | Gst.MessageType.EOS)
    if msg:
        if msg.type == Gst.MessageType.ERROR:
            err, debug_info = msg.parse_error()
            print(f"Error received from element {msg.src.get_name()}: {err.message}")
            print(f"Debugging information: {debug_info or 'none'}")
        elif msg.type == Gst.MessageType.EOS:
            print("End-Of-Stream reached.")
    pipeline.set_state(Gst.State.NULL)

# Create a new thread for message handling beause the bus.timed_pop_filtered() is blocking so if we want to be able to react to cntrl+c we need to run it in a separate thread
message_thread = threading.Thread(target=message_handler, args=(bus,))
message_thread.start()
