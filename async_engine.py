# -*- coding: utf-8 -*-
"""
铁律异步引擎模块
支持异步数据获取、多线程验证执行、并发样本处理
"""
from pathlib import Path
import asyncio
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, Future
from datetime import datetime, date, timedelta
from typing import List, Dict, Optional, Any, Callable, Tuple
from dataclasses import dataclass, field
from collections import deque
from enum import Enum
import threading
import time
import queue
import sys

sys.path.insert(0, str(Path(__file__).parent.parent))

from utils.logger import get_logger

logger = get_logger("AsyncEngine")


@dataclass
class TaskResult:
    """任务结果"""
    task_id: str
    status: str                    # pending/running/completed/failed
    result: Any = None
    error: str = ""
    start_time: float = 0
    end_time: float = 0
    duration: float = 0
    
    def is_completed(self) -> bool:
        return self.status == "completed"
    
    def is_failed(self) -> bool:
        return self.status == "failed"


@dataclass
class ProgressInfo:
    """进度信息"""
    total: int
    completed: int
    running: int
    failed: int
    pending: int
    
    @property
    def progress_pct(self) -> float:
        if self.total == 0:
            return 0
        return self.completed / self.total * 100
    
    def to_dict(self) -> Dict:
        return {
            'total': self.total,
            'completed': self.completed,
            'running': self.running,
            'failed': self.failed,
            'pending': self.pending,
            'progress_pct': f"{self.progress_pct:.1f}%",
        }


@dataclass
class ResourceUsage:
    """资源使用情况"""
    cpu_percent: float
    memory_percent: float
    thread_count: int
    timestamp: str = field(default_factory=lambda: datetime.now().isoformat())
    
    def to_dict(self) -> Dict:
        return {
            'cpu_percent': f"{self.cpu_percent:.1f}%",
            'memory_percent': f"{self.memory_percent:.1f}%",
            'thread_count': self.thread_count,
            'timestamp': self.timestamp,
        }


class TaskQueue:
    """任务队列"""
    
    def __init__(self, max_size: int = 1000):
        self.queue = queue.Queue(maxsize=max_size)
        self.results: Dict[str, TaskResult] = {}
        self.lock = threading.Lock()
        self._task_counter = 0
    
    def put(self, task_func: Callable, task_args: tuple, task_kwargs: dict, task_id: str = None) -> str:
        """添加任务"""
        if task_id is None:
            with self.lock:
                self._task_counter += 1
                task_id = f"task_{self._task_counter}"
        
        task = {
            'id': task_id,
            'func': task_func,
            'args': task_args,
            'kwargs': task_kwargs,
        }
        
        self.queue.put(task)
        
        with self.lock:
            self.results[task_id] = TaskResult(
                task_id=task_id,
                status="pending",
                start_time=time.time(),
            )
        
        return task_id
    
    def get(self, block: bool = True, timeout: float = None) -> Optional[Dict]:
        """获取任务"""
        try:
            return self.queue.get(block=block, timeout=timeout)
        except queue.Empty:
            return None
    
    def mark_completed(self, task_id: str, result: Any):
        """标记完成"""
        with self.lock:
            if task_id in self.results:
                r = self.results[task_id]
                r.status = "completed"
                r.result = result
                r.end_time = time.time()
                r.duration = r.end_time - r.start_time
    
    def mark_failed(self, task_id: str, error: str):
        """标记失败"""
        with self.lock:
            if task_id in self.results:
                r = self.results[task_id]
                r.status = "failed"
                r.error = error
                r.end_time = time.time()
                r.duration = r.end_time - r.start_time
    
    def get_progress(self) -> ProgressInfo:
        """获取进度"""
        with self.lock:
            completed = sum(1 for r in self.results.values() if r.status == "completed")
            running = sum(1 for r in self.results.values() if r.status == "running")
            failed = sum(1 for r in self.results.values() if r.status == "failed")
            pending = sum(1 for r in self.results.values() if r.status == "pending")
            
            return ProgressInfo(
                total=len(self.results),
                completed=completed,
                running=running,
                failed=failed,
                pending=pending,
            )
    
    def get_result(self, task_id: str) -> Optional[TaskResult]:
        """获取任务结果"""
        with self.lock:
            return self.results.get(task_id)
    
    def is_empty(self) -> bool:
        return self.queue.empty()
    
    def size(self) -> int:
        return self.queue.qsize()


class WorkerThread(threading.Thread):
    """工作线程"""
    
    def __init__(self, worker_id: int, task_queue: TaskQueue, results_lock: threading.Lock, results: Dict):
        super().__init__(daemon=True)
        self.worker_id = worker_id
        self.task_queue = task_queue
        self.results_lock = results_lock
        self.results = results
        self._stop_event = threading.Event()
    
    def run(self):
        while not self._stop_event.is_set():
            task = self.task_queue.get(block=True, timeout=1)
            if task is None:
                continue
            
            task_id = task['id']
            func = task['func']
            args = task['args']
            kwargs = task['kwargs']
            
            # 标记为运行中
            with self.results_lock:
                if task_id in self.results:
                    self.results[task_id].status = "running"
            
            try:
                result = func(*args, **kwargs)
                self.task_queue.mark_completed(task_id, result)
            except Exception as e:
                logger.error(f"任务 {task_id} 执行失败: {e}")
                self.task_queue.mark_failed(task_id, str(e))
    
    def stop(self):
        self._stop_event.set()


class AsyncEngine:
    """异步执行引擎"""
    
    def __init__(
        self,
        max_workers: int = 4,
        enable_thread_pool: bool = True,
        enable_process_pool: bool = False,
    ):
        """
        初始化异步引擎
        
        Args:
            max_workers: 最大工作线程数
            enable_thread_pool: 启用线程池
            enable_process_pool: 启用进程池
        """
        self.max_workers = max_workers
        self.task_queue = TaskQueue()
        self.results_lock = threading.Lock()
        self.results: Dict[str, TaskResult] = {}
        
        # 线程池
        self.thread_pool: ThreadPoolExecutor = None
        if enable_thread_pool:
            self.thread_pool = ThreadPoolExecutor(max_workers=max_workers)
        
        # 进程池
        self.process_pool: ProcessPoolExecutor = None
        if enable_process_pool:
            self.process_pool = ProcessPoolExecutor(max_workers=max_workers)
        
        # 工作线程
        self.workers: List[WorkerThread] = []
        
        # 资源监控
        self._monitoring = False
        self._resource_history: deque = deque(maxlen=100)
        
        # 进度回调
        self.progress_callback: Callable = None
    
    def start_workers(self, num_workers: int = None):
        """启动工作线程"""
        if num_workers is None:
            num_workers = self.max_workers
        
        for i in range(num_workers):
            worker = WorkerThread(i, self.task_queue, self.results_lock, self.results)
            worker.start()
            self.workers.append(worker)
        
        logger.info(f"启动 {num_workers} 个工作线程")
    
    def stop_workers(self):
        """停止工作线程"""
        for worker in self.workers:
            worker.stop()
        
        for worker in self.workers:
            worker.join(timeout=5)
        
        self.workers = []
        logger.info("工作线程已停止")
    
    def submit_task(
        self,
        func: Callable,
        *args,
        task_id: str = None,
        **kwargs,
    ) -> str:
        """
        提交任务
        
        Args:
            func: 执行函数
            task_id: 任务ID
            *args, **kwargs: 函数参数
        
        Returns:
            任务ID
        """
        return self.task_queue.put(func, args, kwargs, task_id)
    
    def submit_batch(
        self,
        tasks: List[Tuple[Callable, tuple, dict]],
    ) -> List[str]:
        """
        批量提交任务
        
        Args:
            tasks: [(func, args, kwargs), ...] 列表
        
        Returns:
            任务ID列表
        """
        task_ids = []
        for func, args, kwargs in tasks:
            task_id = self.submit_task(func, *args, task_id=None, **kwargs)
            task_ids.append(task_id)
        
        logger.info(f"批量提交 {len(task_ids)} 个任务")
        return task_ids
    
    def wait_for_completion(
        self,
        task_ids: List[str] = None,
        timeout: float = None,
    ) -> Dict[str, TaskResult]:
        """
        等待任务完成
        
        Args:
            task_ids: 任务ID列表，None表示等待所有
            timeout: 超时时间
        
        Returns:
            结果字典
        """
        start_time = time.time()
        target_ids = set(task_ids) if task_ids else None
        
        while True:
            progress = self.task_queue.get_progress()
            
            # 检查是否全部完成
            if target_ids:
                completed = sum(
                    1 for tid in target_ids 
                    if self.task_queue.get_result(tid) and self.task_queue.get_result(tid).is_completed()
                )
                if completed == len(target_ids):
                    break
            else:
                if progress.pending == 0 and progress.running == 0:
                    break
            
            # 检查超时
            if timeout and (time.time() - start_time) > timeout:
                logger.warning(f"等待超时: {timeout}s")
                break
            
            # 进度回调
            if self.progress_callback:
                self.progress_callback(progress)
            
            time.sleep(0.1)
        
        # 返回结果
        if target_ids:
            return {tid: self.task_queue.get_result(tid) for tid in target_ids}
        return {k: v for k, v in self.task_queue.results.items()}
    
    def get_progress(self) -> ProgressInfo:
        """获取进度"""
        return self.task_queue.get_progress()
    
    def set_progress_callback(self, callback: Callable):
        """设置进度回调"""
        self.progress_callback = callback
    
    def shutdown(self):
        """关闭引擎"""
        self.stop_workers()
        
        if self.thread_pool:
            self.thread_pool.shutdown(wait=True)
        
        if self.process_pool:
            self.process_pool.shutdown(wait=True)
        
        logger.info("异步引擎已关闭")
    
    def get_resource_usage(self) -> ResourceUsage:
        """获取资源使用情况"""
        try:
            import psutil
            process = psutil.Process()
            
            return ResourceUsage(
                cpu_percent=process.cpu_percent(),
                memory_percent=process.memory_percent(),
                thread_count=threading.active_count(),
            )
        except ImportError:
            # 如果没有psutil，使用简化版本
            return ResourceUsage(
                cpu_percent=0,
                memory_percent=0,
                thread_count=threading.active_count(),
            )
    
    def start_monitoring(self, interval: float = 1.0):
        """开始资源监控"""
        self._monitoring = True
        
        def monitor():
            while self._monitoring:
                usage = self.get_resource_usage()
                self._resource_history.append(usage)
                time.sleep(interval)
        
        monitor_thread = threading.Thread(target=monitor, daemon=True)
        monitor_thread.start()
    
    def stop_monitoring(self):
        """停止资源监控"""
        self._monitoring = False
    
    def get_resource_stats(self) -> Dict:
        """获取资源统计"""
        if not self._resource_history:
            return {}
        
        cpu_values = [u.cpu_percent for u in self._resource_history]
        mem_values = [u.memory_percent for u in self._resource_history]
        
        return {
            'cpu_avg': sum(cpu_values) / len(cpu_values),
            'cpu_max': max(cpu_values),
            'memory_avg': sum(mem_values) / len(mem_values),
            'memory_max': max(mem_values),
            'thread_count': threading.active_count(),
            'samples': len(self._resource_history),
        }


class AsyncDataFetcher:
    """异步数据获取器"""
    
    def __init__(self, sync_fetcher=None):
        """
        初始化异步数据获取器
        
        Args:
            sync_fetcher: 同步数据获取器实例
        """
        self.sync_fetcher = sync_fetcher
        self.cache: Dict[str, Any] = {}
        self.cache_lock = threading.Lock()
        self.engine = AsyncEngine(max_workers=8)
    
    async def get_stock_data_async(
        self,
        stock_code: str,
        start_date: date,
        end_date: date,
    ) -> Dict:
        """
        异步获取股票数据
        
        Args:
            stock_code: 股票代码
            start_date: 开始日期
            end_date: 结束日期
        
        Returns:
            股票数据
        """
        cache_key = f"{stock_code}_{start_date}_{end_date}"
        
        # 检查缓存
        with self.cache_lock:
            if cache_key in self.cache:
                return self.cache[cache_key]
        
        # 如果有同步获取器，使用线程池执行
        if self.sync_fetcher:
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(
                self.engine.thread_pool,
                self.sync_fetcher.get_stock_data,
                stock_code, start_date, end_date
            )
        else:
            result = {}
        
        # 写入缓存
        with self.cache_lock:
            self.cache[cache_key] = result
        
        return result
    
    async def get_batch_stock_data_async(
        self,
        stock_codes: List[str],
        start_date: date,
        end_date: date,
    ) -> Dict[str, Dict]:
        """
        批量异步获取股票数据
        
        Args:
            stock_codes: 股票代码列表
            start_date: 开始日期
            end_date: 结束日期
        
        Returns:
            {stock_code: data} 字典
        """
        tasks = []
        
        for code in stock_codes:
            task = self.get_stock_data_async(code, start_date, end_date)
            tasks.append((code, task))
        
        # 并发执行
        results = {}
        for code, task in tasks:
            results[code] = await task
        
        return results
    
    def get_stock_data_sync(
        self,
        stock_code: str,
        start_date: date,
        end_date: date,
    ) -> Dict:
        """同步获取股票数据（使用缓存）"""
        cache_key = f"{stock_code}_{start_date}_{end_date}"
        
        with self.cache_lock:
            if cache_key in self.cache:
                return self.cache[cache_key]
        
        if self.sync_fetcher:
            result = self.sync_fetcher.get_stock_data(stock_code, start_date, end_date)
        else:
            result = {}
        
        with self.cache_lock:
            self.cache[cache_key] = result
        
        return result


class AsyncValidator:
    """异步验证器"""
    
    def __init__(self, validator):
        """
        初始化异步验证器
        
        Args:
            validator: 同步RuleValidator实例
        """
        self.validator = validator
        self.engine = AsyncEngine(max_workers=4)
        self.engine.start_workers()
    
    def _validate_sample_task(self, sample) -> Any:
        """验证单个样本的任务"""
        return self.validator._validate_sample(sample)
    
    async def validate_samples_async(
        self,
        samples: List[Any],
        progress_callback: Callable = None,
    ) -> List[Any]:
        """
        异步验证样本
        
        Args:
            samples: 样本列表
            progress_callback: 进度回调
        
        Returns:
            验证结果列表
        """
        if progress_callback:
            self.engine.set_progress_callback(progress_callback)
        
        # 提交任务
        for sample in samples:
            self.engine.submit_task(
                self._validate_sample_task,
                sample,
                task_id=sample.sample_id if hasattr(sample, 'sample_id') else None,
            )
        
        # 等待完成
        self.engine.wait_for_completion()
        
        # 收集结果
        results = []
        for sample in samples:
            task_id = sample.sample_id if hasattr(sample, 'sample_id') else None
            if task_id:
                result = self.engine.task_queue.get_result(task_id)
                if result and result.is_completed():
                    results.append(result.result)
        
        return results
    
    def shutdown(self):
        """关闭"""
        self.engine.shutdown()


def display_progress_bar(progress: ProgressInfo, width: int = 50):
    """显示进度条"""
    filled = int(width * progress.completed / max(progress.total, 1))
    bar = '█' * filled + '░' * (width - filled)
    
    print(f"\r[{bar}] {progress.progress_pct:.1f}% "
          f"(完成:{progress.completed}/{progress.total} "
          f"运行:{progress.running} 失败:{progress.failed})", end='')
    
    if progress.completed == progress.total:
        print()  # 完成时换行


def integrate_with_validator(validator):
    """
    将异步引擎集成到验证器
    
    Args:
        validator: RuleValidator实例
    """
    # 创建异步数据获取器
    async_fetcher = AsyncDataFetcher(validator.data_fetcher)
    
    # 添加到验证器
    validator.async_engine = AsyncEngine()
    validator.async_fetcher = async_fetcher
    
    # 异步验证方法
    async_validator = AsyncValidator(validator)
    validator.validate_samples_async = async_validator.validate_samples_async
    
    return validator
