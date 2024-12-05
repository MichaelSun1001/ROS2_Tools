import os
import cv2
from cv_bridge import CvBridge
from rosbag2_py import SequentialReader, StorageOptions, ConverterOptions
import rclpy
from rclpy.serialization import deserialize_message
from sensor_msgs.msg import Image, CompressedImage
from rosidl_runtime_py.utilities import get_message

# 定义 db3 文件夹路径
db3_folder = "/media/sax/新加卷/苏州隧道实验-2024年12月2日/实验2/ARS548/db3"

# 定义固定的输出路径
output_base_dir = "/media/sax/新加卷/苏州隧道实验-2024年12月2日/实验2/ARS548/test"

# 动态读取文件夹中的所有 .db3 文件
db3_files = [
    os.path.join(db3_folder, f) for f in os.listdir(db3_folder) if f.endswith(".db3")
]

# 定义图像话题
image_topics = [
    "/usb_cam/image_raw",
    "/usb_cam/image_raw/compressed",
    "/hik_cam_node/hik_camera",
    "/rtsp_camera_relay/image",
]

# 实例化 CvBridge
bridge = CvBridge()


def process_db3_file(db3_file):
    # 检查文件是否存在
    if not os.path.isfile(db3_file):
        print(f"Error: DB3 file '{db3_file}' not found.")
        return

    # 从 db3 文件名生成输出目录
    db3_base_name = os.path.splitext(os.path.basename(db3_file))[0]
    image_output_dir_base = os.path.join(output_base_dir, db3_base_name, "images")

    # 初始化每个话题的 frame_id 字典
    frame_id_dict = {topic: 0 for topic in image_topics}

    print(f"开始处理 db3 文件 '{db3_file}' 中的图像消息...")

    # 设置存储选项
    storage_options = StorageOptions(uri=db3_file, storage_id="sqlite3")
    converter_options = ConverterOptions("", "")

    # 创建读取器
    reader = SequentialReader()
    reader.open(storage_options, converter_options)

    # 读取消息
    while reader.has_next():
        topic, serialized_msg, timestamp = reader.read_next()
        if topic not in image_topics:
            continue

        # 获取消息类型
        msg_type = get_message(topic)
        msg = deserialize_message(serialized_msg, msg_type)

        frame_id = frame_id_dict[topic]  # 获取当前话题的帧序号

        # 为每个图像话题创建独立的目录
        image_topic_dir = os.path.join(
            image_output_dir_base, topic.strip("/").replace("/", "_")
        )
        os.makedirs(image_topic_dir, exist_ok=True)

        # 根据话题类型保存图像
        try:
            if isinstance(msg, Image):
                cv_image = bridge.imgmsg_to_cv2(msg, desired_encoding="bgr8")
                image_file = os.path.join(
                    image_topic_dir, f"image_raw_{frame_id}_{timestamp}.png"
                )
                cv2.imwrite(image_file, cv_image)
                print(f"保存图像文件: {image_file}")

            elif isinstance(msg, CompressedImage):
                cv_image = bridge.compressed_imgmsg_to_cv2(msg, desired_encoding="bgr8")
                compressed_image_file = os.path.join(
                    image_topic_dir,
                    f"image_raw_compressed_{frame_id}_{timestamp}.png",
                )
                cv2.imwrite(compressed_image_file, cv_image)
                print(f"保存压缩图像文件: {compressed_image_file}")

            frame_id_dict[topic] += 1  # 更新当前话题的帧序号

        except Exception as e:
            print(f"Error processing message on topic '{topic}': {e}")


# 处理多个 db3 文件
for db3_file in db3_files:
    process_db3_file(db3_file)
