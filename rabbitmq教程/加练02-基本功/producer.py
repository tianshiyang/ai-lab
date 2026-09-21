TASKS = [
    {"id": "T1", "fail": False},  # 一次成功:observer 看到 run + done
    {"id": "T2", "fail": True},  # 全链路:躺 5 秒回来、再躺 5 秒回来、三连败进死信池
]
