#!/usr/bin/env python3
"""
服务器连接测试工具
用于诊断和验证与hadoop101服务器的连接
"""

import socket
import requests
import subprocess
import sys
from pathlib import Path
import time

# 添加项目路径
sys.path.insert(0, str(Path(__file__).parent))


def test_hostname_resolution():
    """测试主机名解析"""
    print("🔍 测试主机名解析...")

    try:
        import socket
        ip = socket.gethostbyname('hadoop101')
        print(f"✅ hadoop101 解析为: {ip}")
        return ip
    except socket.gaierror:
        print("❌ 无法解析 hadoop101 主机名")
        print("\n💡 解决方案:")
        print("1. 获取服务器IP地址")
        print("2. 在 C:\\Windows\\System32\\drivers\\etc\\hosts 文件中添加:")
        print("   YOUR_SERVER_IP hadoop101")
        print("3. 或直接在.env中使用IP地址替换hadoop101")
        return None


def test_port_connectivity(host, ports):
    """测试端口连通性"""
    print(f"\n🔌 测试 {host} 端口连通性...")

    results = {}
    for service, port in ports.items():
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(5)
            result = sock.connect_ex((host, port))
            sock.close()

            if result == 0:
                print(f"✅ {service} ({port}): 连接成功")
                results[service] = True
            else:
                print(f"❌ {service} ({port}): 连接失败")
                results[service] = False
        except Exception as e:
            print(f"❌ {service} ({port}): 测试异常 - {e}")
            results[service] = False

    return results


def test_web_services(host):
    """测试Web服务可访问性"""
    print(f"\n🌐 测试 {host} Web服务...")

    web_services = {
        "HDFS NameNode WebUI": f"http://{host}:9870",
        "Flink JobManager WebUI": f"http://{host}:8081",
        "Doris Frontend WebUI": f"http://{host}:8060"
    }

    results = {}
    for service, url in web_services.items():
        try:
            response = requests.get(url, timeout=10)
            if response.status_code == 200:
                print(f"✅ {service}: 可访问 ({response.status_code})")
                results[service] = True
            else:
                print(f"⚠️ {service}: HTTP {response.status_code}")
                results[service] = False
        except requests.exceptions.RequestException:
            print(f"❌ {service}: 连接被拒绝")
            results[service] = False
        except requests.exceptions.Timeout:
            print(f"❌ {service}: 连接超时")
            results[service] = False
        except Exception as e:
            print(f"❌ {service}: 访问异常 - {e}")
            results[service] = False

    return results


def test_hive_connection():
    """测试Hive连接"""
    print("\n🐝 测试Hive连接...")

    try:
        # 尝试导入pyhive
        from pyhive import hive
        print("✅ pyhive 库已安装")

        # 尝试连接
        try:
            conn = hive.Connection(
                host='hadoop101',
                port=10000,
                username='bigdata',
                password='gqdw8862',
                auth='CUSTOM'
            )

            cursor = conn.cursor()
            cursor.execute("SHOW DATABASES")
            databases = cursor.fetchall()

            print(f"✅ Hive连接成功！找到 {len(databases)} 个数据库:")
            for db in databases[:5]:  # 显示前5个
                print(f"   - {db[0]}")

            if len(databases) > 5:
                print(f"   ... 还有 {len(databases) - 5} 个数据库")

            cursor.close()
            conn.close()
            return True

        except Exception as e:
            print(f"❌ Hive连接失败: {e}")
            return False

    except ImportError:
        print("❌ pyhive 库未安装")
        print("💡 安装命令: pip install pyhive[hive] sasl thrift thrift-sasl")
        return False


def test_hdfs_connection():
    """测试HDFS连接"""
    print("\n📁 测试HDFS连接...")

    # 测试WebHDFS API
    try:
        webhdfs_url = "http://hadoop101:9870/webhdfs/v1/?op=LISTSTATUS"
        response = requests.get(webhdfs_url, timeout=10)

        if response.status_code == 200:
            data = response.json()
            if 'FileStatuses' in data:
                print("✅ HDFS WebHDFS API 连接成功")
                files = data['FileStatuses']['FileStatus']
                print(f"   根目录包含 {len(files)} 个文件/目录")
                return True
            else:
                print(f"⚠️ HDFS WebHDFS API 响应异常: {data}")
                return False
        else:
            print(f"❌ HDFS WebHDFS API HTTP {response.status_code}")
            return False

    except Exception as e:
        print(f"❌ HDFS连接测试失败: {e}")
        return False


def generate_hosts_file_entry():
    """生成hosts文件条目"""
    print("\n📝 如果主机名解析失败，请按以下步骤操作:")
    print("1. 在服务器上执行命令获取IP地址:")
    print("   ip addr show | grep 'inet ' | grep -v '127.0.0.1'")
    print("\n2. 编辑Windows hosts文件:")
    print("   文件位置: C:\\Windows\\System32\\drivers\\etc\\hosts")
    print("   添加行: YOUR_SERVER_IP hadoop101")
    print("\n3. 或者直接在.env文件中使用IP地址替换hadoop101")


def main():
    """主测试函数"""
    print("🧪 首信云数据底座 - 服务器连接诊断工具")
    print("=" * 60)

    # 1. 测试主机名解析
    server_ip = test_hostname_resolution()

    if not server_ip:
        generate_hosts_file_entry()
        return

    # 2. 测试端口连通性
    ports = {
        "HDFS NameNode RPC": 8020,
        "HDFS NameNode WebUI": 9870,
        "Hive Server2": 10000,
        "Flink JobManager": 8081,
        "Doris Frontend": 8060,
        "Redis": 6379
    }

    port_results = test_port_connectivity('hadoop101', ports)

    # 3. 测试Web服务
    web_results = test_web_services('hadoop101')

    # 4. 测试Hive连接
    hive_result = test_hive_connection()

    # 5. 测试HDFS连接
    hdfs_result = test_hdfs_connection()

    # 6. 生成诊断报告
    print("\n📊 连接诊断报告")
    print("=" * 40)

    print(f"主机名解析: {'✅ 成功' if server_ip else '❌ 失败'}")

    successful_ports = sum(1 for success in port_results.values() if success)
    total_ports = len(port_results)
    print(f"端口连通性: {successful_ports}/{total_ports} 成功")

    successful_web = sum(1 for success in web_results.values() if success)
    total_web = len(web_results)
    print(f"Web服务: {successful_web}/{total_web} 可访问")

    print(f"Hive连接: {'✅ 成功' if hive_result else '❌ 失败'}")
    print(f"HDFS连接: {'✅ 成功' if hdfs_result else '❌ 失败'}")

    # 7. 提供建议
    print("\n💡 建议:")
    if not server_ip:
        print("- 首先解决主机名解析问题")
    elif successful_ports < total_ports:
        print("- 检查服务器防火墙设置")
        print("- 确认服务是否正在运行")
    elif not hive_result:
        print("- 安装Hive客户端依赖: pip install pyhive[hive]")
        print("- 检查Hive用户名密码")
    elif successful_ports == total_ports and hive_result:
        print("- ✅ 连接配置正确，可以启动应用！")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n🛑 测试已取消")
    except Exception as e:
        print(f"❌ 测试出错: {e}")