import os
import pandas as pd
from sensor_msgs.msg import PointCloud, PointCloud2
from rosbag2_py import SequentialReader, StorageOptions, ConverterOptions
from rclpy.serialization import deserialize_message
from sensor_msgs_py import point_cloud2 as pc2

# 主文件夹路径，包含多个 DB3 文件
parent_dir = "/media/sax/新加卷/db3"
# 输出目录路径
output_parent_dir = "/media/sax/新加卷/processed_db3"

# # 需要读取的指定话题列表
# topics_to_process = [
#     # "/usb_cam_1/compressed",  # 话题1
#     # "/camera/depth/image_raw",  # 话题2
#     # "/hugin_raf_1/radar_data",
#     # # 添加其他你需要处理的话题
# ]

# 确保输出目录存在
os.makedirs(output_parent_dir, exist_ok=True)


def save_pointcloud_data(data, columns, output_csv, output_txt):
    """将点云数据保存为 CSV 和 TXT 文件"""
    if data:
        try:
            # 保存为 CSV 文件
            df = pd.DataFrame(data, columns=columns)
            df.to_csv(output_csv, index=False)
            print(f"点云数据已保存到 {output_csv}")

            # 保存为 TXT 文件
            df.to_csv(output_txt, sep=" ", index=False, header=True)
            print(f"点云数据已保存到 {output_txt}")
        except Exception as e:
            print(f"Error saving PointCloud data: {e}")
    else:
        # print("没有点云数据需要保存")
        print("")


def process_db3_file(db3_file, output_dir):
    """处理单个 DB3 文件并提取并保存点云数据"""
    if not os.path.isfile(db3_file):
        print(f"Error: DB3 file '{db3_file}' not found.")
        return

    # 直接使用 db3 文件的基础名称创建输出目录
    db3_base_name = os.path.splitext(os.path.basename(db3_file))[0]

    # 设置存储选项
    storage_options = StorageOptions(uri=db3_file, storage_id="sqlite3")
    converter_options = ConverterOptions("", "")
    reader = SequentialReader()
    reader.open(storage_options, converter_options)

    print(f"开始处理 db3 文件 '{db3_file}' 中的点云数据...")

    # 为每个 topic 初始化独立的 frame_id
    topic_frame_counters = {}

    # 获取所有话题类型
    topics_metadata = reader.get_all_topics_and_types()
    topic_types = {topic.name: topic.type for topic in topics_metadata}

    # 用于存储点云数据
    all_data_pc2 = []
    all_data_pc = []
    columns_pc2 = None  # PointCloud2 的列名
    columns_pc = None  # PointCloud 的列名

    # 读取消息
    while reader.has_next():
        topic, serialized_msg, timestamp_ns = reader.read_next()

        # # 注意，如果想指定话题进行读取，请开启这部分代码。
        # # 判断当前话题是否在需要处理的列表中
        # if topic not in topics_to_process:
        #     print(f"No topic in the list !!! ")
        #     print(f"No topic in the list !!! ")
        #     print(f"No topic in the list !!! ")
        #     return  # 如果当前话题不在指定列表中，跳过此话题

        # 判断消息类型并反序列化
        msg_type = topic_types.get(topic)
        if not msg_type:
            print(f"Warning: Unknown message type for topic '{topic}'")
            continue

        if msg_type not in [
            "sensor_msgs/msg/PointCloud2",
            "sensor_msgs/msg/PointCloud",
        ]:
            print(f"Skipping non-pointcloud topic '{topic}'")
            continue
        else:
            print(f"Processing topic: {topic}")

        try:
            if msg_type == "sensor_msgs/msg/PointCloud2":  # 处理 PointCloud2 消息
                msg = deserialize_message(serialized_msg, PointCloud2)
                if columns_pc2 is None:
                    columns_pc2 = ["frame_id", "timestamp"] + [
                        field.name for field in msg.fields
                    ]

                # 提取时间戳并格式化为 "sec.nsec"
                timestamp_sec = msg.header.stamp.sec
                timestamp_nsec = msg.header.stamp.nanosec
                timestamp = f"{timestamp_sec}.{timestamp_nsec:09d}"

                # 提取点云数据
                pc_gen = pc2.read_points(
                    msg,
                    field_names=[field.name for field in msg.fields],
                    skip_nans=True,
                )

                frame_id_pc2 = topic_frame_counters.get(topic, 0)
                topic_frame_counters[topic] = frame_id_pc2 + 1

                # 存储每个点的数据
                for point in pc_gen:
                    all_data_pc2.append((frame_id_pc2, timestamp) + tuple(point))

            elif msg_type == "sensor_msgs/msg/PointCloud":  # 处理 PointCloud 消息
                msg = deserialize_message(serialized_msg, PointCloud)
                if columns_pc is None:
                    columns_pc = ["frame_id", "timestamp"] + [
                        field.name for field in msg.fields
                    ]

                # 提取时间戳并格式化为 "sec.nsec"
                timestamp_sec = msg.header.stamp.sec
                timestamp_nsec = msg.header.stamp.nanosec
                timestamp = f"{timestamp_sec}.{timestamp_nsec:09d}"

                # 提取点云数据
                pc_gen = pc2.read_points(
                    msg,
                    field_names=[field.name for field in msg.fields],
                    skip_nans=True,
                )

                frame_id_pc = topic_frame_counters.get(topic, 0)
                topic_frame_counters[topic] = frame_id_pc + 1

                # 存储每个点的数据
                for point in pc_gen:
                    all_data_pc.append((frame_id_pc, timestamp) + tuple(point))

        except Exception as e:
            print(f"Error processing message from topic '{topic}': {e}")
            continue

    # 为每个话题创建输出子目录
    topic_name = topic.replace("/", "_")  # 将话题名称中的斜杠替换为下划线
    topic_output_dir = os.path.join(output_dir, topic_name)

    # 确保话题子目录存在
    os.makedirs(topic_output_dir, exist_ok=True)

    # 保存 PointCloud2 数据
    save_pointcloud_data(
        all_data_pc2,
        columns_pc2,
        os.path.join(topic_output_dir, "PointCloud2.csv"),
        os.path.join(topic_output_dir, "PointCloud2.txt"),
    )

    # 保存 PointCloud 数据
    save_pointcloud_data(
        all_data_pc,
        columns_pc,
        os.path.join(topic_output_dir, "PointCloud.csv"),
        os.path.join(topic_output_dir, "PointCloud.txt"),
    )


def process_all_db3_files(parent_dir, output_parent_dir):
    """处理主目录下所有 DB3 文件"""
    for root, dirs, files in os.walk(parent_dir):
        for file in files:
            if file.endswith(".db3"):
                db3_file_path = os.path.join(root, file)

                # 创建输出目录，与输入目录结构一致
                relative_path = os.path.relpath(root, parent_dir)
                output_dir = os.path.join(output_parent_dir, relative_path)
                os.makedirs(output_dir, exist_ok=True)

                print(f"开始处理 {db3_file_path} ...")
                process_db3_file(db3_file_path, output_dir)


# 处理所有 DB3 文件
process_all_db3_files(parent_dir, output_parent_dir)
