import os
import pandas as pd
from sensor_msgs.msg import PointCloud2
from rosbag2_py import SequentialReader, StorageOptions, ConverterOptions
from sensor_msgs_py import point_cloud2 as pc2
from rclpy.serialization import deserialize_message

# 定义要读取的 topic 和对应的消息类型
topics_to_check = {
    "/ars548": PointCloud2,
    "/hugin_raf_1/radar_data": PointCloud2,
}

# 主文件夹路径，里面包含多个子文件夹
parent_dir = "/media/sax/新加卷/db3"
# 输出目录路径
output_parent_dir = "/media/sax/新加卷/processed_db3"

# 确保输出目录存在
os.makedirs(output_parent_dir, exist_ok=True)


def process_db3_file(db3_file, output_dir):
    # 检查文件是否存在
    if not os.path.isfile(db3_file):
        print(f"Error: DB3 file '{db3_file}' not found.")
        return

    # 从 db3 文件名生成 CSV 和 TXT 文件名
    db3_base_name = os.path.splitext(os.path.basename(db3_file))[0]
    output_csv_pc2 = os.path.join(output_dir, f"{db3_base_name}_PointCloud2.csv")
    output_txt_pc2 = os.path.join(output_dir, f"{db3_base_name}_PointCloud2.txt")

    # 用于存储 PointCloud2 数据
    all_data_pc2 = []
    columns_pc2 = None
    frame_id_pc2 = -1  # PointCloud2

    # 设置存储选项
    storage_options = StorageOptions(uri=db3_file, storage_id="sqlite3")
    converter_options = ConverterOptions("", "")

    # 创建读取器
    reader = SequentialReader()
    reader.open(storage_options, converter_options)

    print(f"开始处理 db3 文件 '{db3_file}' 中的 PointCloud2 消息...")

    # 读取消息
    while reader.has_next():
        topic, serialized_msg, timestamp_ns = reader.read_next()
        print(f"Reading topic: {topic}")  # 调试输出：显示正在读取的 topic
        if topic not in topics_to_check:
            continue

        # 处理 PointCloud2 消息
        if topic in topics_to_check:
            # 反序列化消息
            msg = deserialize_message(serialized_msg, PointCloud2)

            print(
                f"Processing PointCloud2 message from topic '{topic}'"
            )  # 调试输出：正在处理消息
            frame_id_pc2 += 1  # 递增 PointCloud2 的帧序号

            if columns_pc2 is None:  # 初始化列名
                columns_pc2 = ["frame_id", "timestamp"] + [
                    field.name for field in msg.fields
                ]

            # 提取时间戳并格式化为 "sec.nsec"
            timestamp_sec = msg.header.stamp.sec
            timestamp_nsec = msg.header.stamp.nanosec
            timestamp = f"{timestamp_sec}.{timestamp_nsec:09d}"  # 格式化时间戳

            # 提取所有点的数据
            pc_gen = pc2.read_points(
                msg,
                field_names=[field.name for field in msg.fields],
                skip_nans=True,
            )

            # 将格式化后的时间戳和每个点的数据一起存储
            for point in pc_gen:
                all_data_pc2.append((frame_id_pc2, timestamp) + tuple(point))

    # 保存 PointCloud2 数据到文件
    if all_data_pc2:
        try:
            # 确保文件夹路径有效
            if not os.path.exists(output_dir):
                os.makedirs(output_dir)
            # 将数据保存到 CSV 文件
            df_pc2 = pd.DataFrame(all_data_pc2, columns=columns_pc2)
            df_pc2.to_csv(output_csv_pc2, index=False)  # 禁用索引
            print(f"PointCloud2 数据已保存到 {output_csv_pc2}")

            # 将数据保存到 TXT 文件
            df_pc2.to_csv(output_txt_pc2, sep=" ", index=False, header=True)
            print(f"PointCloud2 数据已保存到 {output_txt_pc2}")
        except Exception as e:
            print(f"Error saving PointCloud2 data: {e}")
    else:
        print(f"No PointCloud2 data found in {db3_file}")


def process_all_db3_files(parent_dir, output_parent_dir):
    # 遍历父目录下的所有子文件夹
    for root, dirs, files in os.walk(parent_dir):
        # 只处理 db3 文件
        for file in files:
            if file.endswith(".db3"):
                db3_file_path = os.path.join(root, file)

                # 创建一个与 db3 文件所在文件夹同名的输出文件夹
                relative_path = os.path.relpath(root, parent_dir)
                output_dir = os.path.join(output_parent_dir, relative_path)

                # 确保输出文件夹存在
                os.makedirs(output_dir, exist_ok=True)

                print(f"开始处理 {db3_file_path} ...")
                process_db3_file(db3_file_path, output_dir)


# 示例：调用函数来处理所有 db3 文件
process_all_db3_files(parent_dir, output_parent_dir)
