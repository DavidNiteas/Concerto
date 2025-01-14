import asyncio
import nest_asyncio
nest_asyncio.apply()
from .base_tools import  ProgressManager,get_kv_pairs
from .backend import HAS_TRIO_BACKEND,HAS_CURIO_BACKEND
if HAS_TRIO_BACKEND:
    from .backend import trio
if HAS_CURIO_BACKEND:
    from .backend import curio
from concurrent.futures import ThreadPoolExecutor, Future, as_completed
from rich.progress import track
from typing import List,Tuple,Dict,Union,Optional,Callable,Any,Literal,TypeVar,Hashable

def use_thread(func):
    
    def key_wrapper(
        key: Union[Hashable, int],
        args: Tuple[Any,...],
        kwargs: Dict[str, Any],
    ):
        return (key, func(*args, **kwargs))
    
    async def thread_submitter(
        key: Union[Hashable, int], 
        thread_pool: ThreadPoolExecutor, 
        future_list: List[Future], 
        args: Tuple[Any], 
        kwargs: Dict[str,Any],
        progress: Union[ProgressManager,None] = None,
    ):
        future = thread_pool.submit(key_wrapper, key, args, kwargs)
        future_list.append(future)
        
        if progress is not None:
            progress.update(func.__name__, advance=1)
            
    thread_submitter.__inner_func_name__ = func.__name__
        
    return thread_submitter

if HAS_TRIO_BACKEND:
    async def trio_backend_thread(
        func: Callable, 
        func_inps: Union[
            Dict[Hashable, Tuple[tuple,dict]],
            List[Tuple[tuple,dict]],
        ],
        thread_pool: ThreadPoolExecutor, 
        future_list: List[Future], 
        progress: Union[ProgressManager,None] = None,
    ):
        async with trio.open_nursery() as nursery:
            for key, (args, kwargs) in get_kv_pairs(func_inps):
                nursery.start_soon(func, key, thread_pool, future_list, args, kwargs, progress)

if HAS_CURIO_BACKEND:
    async def curio_backend_thread(
        func: Callable, 
        func_inps: Union[
            Dict[Hashable, Tuple[tuple,dict]],
            List[Tuple[tuple,dict]],
        ],
        thread_pool: ThreadPoolExecutor, 
        future_list: List[Future], 
        progress: Union[ProgressManager,None] = None,
    ):
        async with curio.TaskGroup() as task_group:
            for key, (args, kwargs) in get_kv_pairs(func_inps):
                task_group.spawn(func, key, thread_pool, future_list, args, kwargs, progress)

async def asyncio_backend_thread(
    func: Callable, 
    func_inps: Union[
        Dict[Hashable, Tuple[tuple,dict]],
        List[Tuple[tuple,dict]],
    ],
    thread_pool: ThreadPoolExecutor, 
    future_list: List[Future], 
    progress: Union[ProgressManager,None] = None,
):
    tasks = []
    for key, (args, kwargs) in get_kv_pairs(func_inps):
        tasks.append(func(func, key, thread_pool, future_list, args, kwargs, progress))
    await asyncio.gather(*tasks)

def run_threads(
    func: Callable,
    func_inps: Union[
        Dict[Hashable, Tuple[tuple,dict]],
        List[Tuple[tuple,dict]],
    ],
    threads: Union[ThreadPoolExecutor,int,None], 
    future_list: Optional[List[Future]] = None,
    backend: Union[
        Literal['trio','curio','asyncio'],
        Tuple[Literal['trio','curio','asyncio'],Callable[
            [
                Callable,
                Union[
                    Dict[Hashable, Tuple[tuple,dict]],
                    List[Tuple[tuple,dict]],
                ],
                Dict[Hashable,Any],
            ],None
        ]],
    ] = 'trio',
    progress: Union[ProgressManager,bool] = True,
    description: str = None,
) -> Tuple[List[Future],ThreadPoolExecutor]:
    need_close = False
    if isinstance(threads, int) or threads is None:
        thread_pool = ThreadPoolExecutor(threads)
    else:
        thread_pool = threads
    if future_list is None:
        future_list = []
    if backend == 'trio':
        if not HAS_TRIO_BACKEND:
            raise ImportError('trio backend is not available')
        coroutine = trio_backend_thread
    elif backend == 'curio':
        if not HAS_CURIO_BACKEND:
            raise ImportError('curio backend is not available')
        coroutine = curio_backend_thread
    elif backend == 'asyncio':
        coroutine = asyncio_backend_thread
    else:
        backend,coroutine = backend
    need_close = False
    if progress is True:
        progress = ProgressManager()
        progress.start()
        need_close = True
    elif progress is False:
        progress = None
    if progress is not None:
        progress.add_task(
            task_name=func.__inner_func_name__,total=len(func_inps),description=description
        )
    if backend == 'trio':
        if not HAS_TRIO_BACKEND:
            raise ImportError('trio backend is not available')
        trio.run(coroutine, func, func_inps, thread_pool, future_list, progress)
    elif backend == 'curio':
        if not HAS_CURIO_BACKEND:
            raise ImportError('curio backend is not available')
        curio.run(coroutine, func, func_inps, thread_pool, future_list, progress)
    elif backend == 'asyncio':
        asyncio.run(coroutine(func, func_inps, thread_pool, future_list, progress))
    if need_close:
        progress.stop()
    return future_list,thread_pool

class AsCompleted:
    
    def __init__(
        self, 
        futures: List[Future], 
        executor: ThreadPoolExecutor,
        wait: bool = True,
        use_progress: bool = False,
        description: Optional[str] = None,
    ):
        self.futures = futures
        self.executor = executor
        self.wait = wait
        self.use_progress = use_progress
        self.description = description

    def __enter__(self):
        if self.use_progress:
            return as_completed(track(self.futures,description=self.description))
        else:
            return as_completed(self.futures)

    def __exit__(self, exc_type, exc_value, traceback):
        self.executor.shutdown(wait=self.wait)
        return False