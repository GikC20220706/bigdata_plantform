# test_overview_endpoints.py
import asyncio
import requests
import json
from datetime import datetime

BASE_URL = "http://localhost:8000"  # 修改为你的FastAPI服务地址


def test_overview_endpoints():
    """测试所有Overview相关的API端点"""
    print("=== 测试Overview API端点 (使用真实集群数据) ===")

    endpoints = [
        {"url": "/api/overview/", "name": "数据总览"},
        {"url": "/api/overview/stats", "name": "统计信息"},
        {"url": "/api/overview/clusters", "name": "集群状态"},
        {"url": "/api/overview/clusters/real-time", "name": "实时集群指标"},
        {"url": "/api/overview/tasks/today", "name": "今日任务"},
        {"url": "/api/overview/storage", "name": "存储信息"},
        {"url": "/api/overview/quality", "name": "数据质量指标"},
        {"url": "/api/overview/health", "name": "系统健康状态"},
        {"url": "/api/overview/business-systems", "name": "业务系统列表"},
        {"url": "/api/overview/database-layers", "name": "数据库分层统计"}
    ]

    results = []

    for endpoint in endpoints:
        print(f"\n{'=' * 60}")
        print(f"测试: {endpoint['name']}")
        print(f"URL: {endpoint['url']}")

        try:
            response = requests.get(f"{BASE_URL}{endpoint['url']}", timeout=30)

            if response.status_code == 200:
                data = response.json()
                print(f"✅ 请求成功 (状态码: {response.status_code})")

                # 分析响应数据
                result = analyze_response(endpoint, data)
                results.append(result)

                # 显示关键信息
                display_key_info(endpoint, data)

            else:
                print(f"❌ 请求失败 (状态码: {response.status_code})")
                try:
                    error_data = response.json()
                    print(f"错误详情: {error_data}")
                except:
                    print(f"错误响应: {response.text}")

        except requests.exceptions.Timeout:
            print("❌ 请求超时")
        except requests.exceptions.ConnectionError:
            print("❌ 连接失败 - 请确保FastAPI服务正在运行")
        except Exception as e:
            print(f"❌ 请求异常: {str(e)}")

    # 生成测试报告
    generate_test_report(results)


def analyze_response(endpoint, data):
    """分析响应数据"""
    result = {
        "endpoint": endpoint['name'],
        "url": endpoint['url'],
        "success": True,
        "data_sources": [],
        "real_data_count": 0,
        "mock_data_count": 0,
        "error_count": 0,
        "key_metrics": {}
    }

    # 递归查找数据源信息
    data_sources = find_data_sources(data)
    result["data_sources"] = data_sources

    # 统计数据源类型
    for source in data_sources:
        if source == "real":
            result["real_data_count"] += 1
        elif source == "mock":
            result["mock_data_count"] += 1
        elif source == "error":
            result["error_count"] += 1

    return result


def find_data_sources(obj, sources=None):
    """递归查找data_source字段"""
    if sources is None:
        sources = []

    if isinstance(obj, dict):
        if "data_source" in obj:
            sources.append(obj["data_source"])
        for value in obj.values():
            find_data_sources(value, sources)
    elif isinstance(obj, list):
        for item in obj:
            find_data_sources(item, sources)

    return sources


def display_key_info(endpoint, data):
    """显示关键信息"""
    if endpoint['name'] == "数据总览":
        if 'clusters' in data:
            clusters = data['clusters']
            print(f"   📊 集群数量: {len(clusters)}")
            for cluster in clusters:
                status = cluster.get('status', 'unknown')
                nodes = f"{cluster.get('active_nodes', 0)}/{cluster.get('total_nodes', 0)}"
                data_source = cluster.get('data_source', 'unknown')
                print(f"      - {cluster.get('name', 'Unknown')}: {status} ({nodes} 节点) [{data_source}]")

    elif endpoint['name'] == "集群状态":
        if 'data' in data:
            clusters = data['data']
            print(f"   📊 集群状态详情:")
            for cluster in clusters:
                name = cluster.get('name', 'Unknown')
                status = cluster.get('status', 'unknown')
                cpu = cluster.get('cpu_usage', 0)
                memory = cluster.get('memory_usage', 0)
                data_source = cluster.get('data_source', 'unknown')
                print(f"      - {name}: {status} (CPU: {cpu}%, Memory: {memory}%) [{data_source}]")

    elif endpoint['name'] == "实时集群指标":
        if 'data' in data:
            clusters_data = data['data']
            print(f"   ⚡ 实时指标:")
            for cluster_type, metrics in clusters_data.items():
                name = metrics.get('cluster_name', cluster_type)
                health = metrics.get('health_score', 0)
                nodes = f"{metrics.get('active_nodes', 0)}/{metrics.get('total_nodes', 0)}"
                data_source = metrics.get('data_source', 'unknown')
                print(f"      - {name}: 健康度 {health}% ({nodes} 节点) [{data_source}]")

    elif endpoint['name'] == "系统健康状态":
        if 'overall_status' in data:
            overall = data['overall_status']
            components = data.get('components', {})
            alerts = data.get('alerts', [])
            print(f"   🏥 总体状态: {overall}")
            print(f"   🔍 组件状态:")
            for comp_name, comp_data in components.items():
                status = comp_data.get('status', 'unknown')
                message = comp_data.get('message', 'No message')
                data_source = comp_data.get('data_source', 'unknown')
                print(f"      - {comp_name}: {status} - {message} [{data_source}]")
            if alerts:
                print(f"   ⚠️ 活跃告警: {len(alerts)} 个")

    elif endpoint['name'] == "今日任务":
        if 'data' in data:
            task_data = data['data']
            if 'tasks' in task_data:
                tasks = task_data['tasks']
                cluster_status = task_data.get('cluster_status', {})
                print(f"   📋 今日任务: {len(tasks)} 个")
                print(f"   🔄 运行中: {task_data.get('running_tasks', 0)} 个")
                print(f"   ✅ 已完成: {task_data.get('completed_tasks', 0)} 个")
                print(f"   ❌ 失败: {task_data.get('failed_tasks', 0)} 个")
                print(f"   🖥️ 集群状态:")
                for cluster, status in cluster_status.items():
                    healthy = "健康" if status.get('healthy') else "异常"
                    nodes = f"{status.get('active_nodes', 0)}/{status.get('total_nodes', 0)}"
                    print(f"      - {cluster}: {healthy} ({nodes} 节点)")

    elif endpoint['name'] == "数据质量指标":
        if 'overall_score' in data:
            score = data['overall_score']
            completeness = data.get('completeness', 0)
            accuracy = data.get('accuracy', 0)
            consistency = data.get('consistency', 0)
            timeliness = data.get('timeliness', 0)
            print(f"   ⭐ 总体评分: {score}%")
            print(f"   📊 详细指标:")
            print(f"      - 完整性: {completeness}%")
            print(f"      - 准确性: {accuracy}%")
            print(f"      - 一致性: {consistency}%")
            print(f"      - 及时性: {timeliness}%")


def generate_test_report(results):
    """生成测试报告"""
    print(f"\n{'=' * 60}")
    print("=== 测试报告 ===")

    total_tests = len(results)
    successful_tests = len([r for r in results if r['success']])

    print(f"总测试数: {total_tests}")
    print(f"成功: {successful_tests}")
    print(f"失败: {total_tests - successful_tests}")

    # 数据源统计
    total_real_data = sum(r['real_data_count'] for r in results)
    total_mock_data = sum(r['mock_data_count'] for r in results)
    total_error_data = sum(r['error_count'] for r in results)

    print(f"\n数据源分析:")
    print(f"使用真实数据: {total_real_data} 个数据点")
    print(f"使用模拟数据: {total_mock_data} 个数据点")
    print(f"数据错误: {total_error_data} 个数据点")

    # 详细结果
    print(f"\n详细结果:")
    for result in results:
        endpoint_name = result['endpoint']
        real_count = result['real_data_count']
        mock_count = result['mock_data_count']
        error_count = result['error_count']

        status_icon = "✅" if result['success'] else "❌"
        data_status = ""

        if real_count > 0 and mock_count == 0 and error_count == 0:
            data_status = "🟢 全部真实数据"
        elif real_count > 0 and (mock_count > 0 or error_count > 0):
            data_status = "🟡 混合数据"
        elif mock_count > 0 and real_count == 0:
            data_status = "🔵 模拟数据"
        elif error_count > 0:
            data_status = "🔴 数据错误"
        else:
            data_status = "⚪ 无数据源信息"

        print(f"  {status_icon} {endpoint_name}: {data_status}")

    # 建议
    print(f"\n建议:")
    if total_real_data == 0:
        print("- ❗ 所有API都在使用模拟数据，请检查:")
        print("  1. .env 配置: IS_LOCAL_DEV=false, MOCK_DATA_MODE=false")
        print("  2. SSH连接是否正常")
        print("  3. metrics_collector 是否正常工作")
    elif total_mock_data > 0:
        print("- ⚠️ 部分API仍在使用模拟数据，可能原因:")
        print("  1. 部分集群节点连接失败")
        print("  2. 某些服务组件异常")
    else:
        print("- 🎉 所有API都在使用真实数据，系统运行正常！")

    if total_error_data > 0:
        print(f"- 🚨 发现 {total_error_data} 个数据错误，请检查日志排查问题")


def test_refresh_endpoint():
    """测试刷新端点"""
    print(f"\n{'=' * 60}")
    print("测试数据刷新功能...")

    try:
        response = requests.post(f"{BASE_URL}/api/overview/refresh", timeout=30)

        if response.status_code == 200:
            data = response.json()
            print("✅ 数据刷新成功")

            if 'data' in data:
                refresh_data = data['data']
                print(f"   刷新时间: {refresh_data.get('refresh_time', 'Unknown')}")
                print(f"   集群数量: {refresh_data.get('clusters_count', 0)}")
                print(f"   真实数据集群: {refresh_data.get('real_data_clusters', 0)}")

                cluster_summary = refresh_data.get('cluster_summary', {})
                print(f"   集群状态:")
                for cluster_type, info in cluster_summary.items():
                    nodes = f"{info.get('active_nodes', 0)}/{info.get('total_nodes', 0)}"
                    data_source = info.get('data_source', 'unknown')
                    print(f"     - {cluster_type}: {nodes} 节点 [{data_source}]")
        else:
            print(f"❌ 刷新失败 (状态码: {response.status_code})")

    except Exception as e:
        print(f"❌ 刷新异常: {str(e)}")


async def test_direct_metrics():
    """直接测试metrics_collector"""
    print(f"\n{'=' * 60}")
    print("直接测试 metrics_collector...")

    try:
        import sys
        import os
        sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

        from app.utils.metrics_collector import metrics_collector

        for cluster_type in ['hadoop', 'flink', 'doris']:
            print(f"\n测试 {cluster_type} 集群...")
            try:
                cluster_metrics = await metrics_collector.get_cluster_metrics(cluster_type)

                data_source = cluster_metrics.get('data_source', 'unknown')
                active_nodes = cluster_metrics.get('active_nodes', 0)
                total_nodes = cluster_metrics.get('total_nodes', 0)
                cpu_usage = cluster_metrics.get('cpu_usage', 0)
                memory_usage = cluster_metrics.get('memory_usage', 0)

                print(f"✅ {cluster_type} - 数据源: {data_source}")
                print(f"   节点: {active_nodes}/{total_nodes}")
                print(f"   CPU: {cpu_usage}%, Memory: {memory_usage}%")

            except Exception as e:
                print(f"❌ {cluster_type} 获取失败: {e}")

    except Exception as e:
        print(f"❌ 无法导入 metrics_collector: {e}")


def main():
    """主测试函数"""
    print("Overview API 真实数据测试")
    print("=" * 60)

    # 1. 测试Overview API端点
    test_overview_endpoints()

    # 2. 测试刷新功能
    test_refresh_endpoint()

    # 3. 直接测试metrics_collector
    asyncio.run(test_direct_metrics())

    print(f"\n{'=' * 60}")
    print("测试完成!")
    print("\n下一步:")
    print("1. 如果看到大量真实数据，说明系统正常工作")
    print("2. 如果仍有模拟数据，请检查集群连接")
    print("3. 查看FastAPI日志获取更多调试信息")


if __name__ == "__main__":
    main()