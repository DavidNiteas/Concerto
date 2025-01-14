import asyncio
import nest_asyncio
nest_asyncio.apply()
from .base_tools import  ProgressManager,get_kv_pairs
from .backend import HAS_TRIO_BACKEND,HAS_CURIO_BACKEND
if HAS_TRIO_BACKEND:
    from .backend import trio
if HAS_CURIO_BACKEND:
    from .backend import curio
from typing import List,Tuple,Dict,Union,Optional,Callable,Any,Literal,TypeVar,Hashable

def use_coroutine(func):
    
    async def coroutine(
        key: Union[Hashable, int], 
        data_dict: Dict[Hashable,Dict[int,Any]], 
        args: Tuple[Any], 
        kwargs: Dict[str,Any],
        progress: Union[ProgressManager,None] = None,
    ):
        data_dict[key] = func(*args, **kwargs)
        
        if progress is not None:
            progress.update(func.__name__, advance=1)
            
    coroutine.__inner_func_name__ = func.__name__
        
    return coroutine

if HAS_TRIO_BACKEND:
    async def trio_backend_coroutine(
        func: Callable, 
        func_inps: Union[
            Dict[Hashable, Tuple[tuple,dict]],
            List[Tuple[tuple,dict]],
        ],
        data_dict: Dict[Hashable,Any],
        progress: Union[ProgressManager,None] = None,
    ):
        async with trio.open_nursery() as nursery:
            for key, (args, kwargs) in get_kv_pairs(func_inps):
                nursery.start_soon(func, key, data_dict, args, kwargs, progress)

if HAS_CURIO_BACKEND:
    async def curio_backend_coroutine(
        func: Callable, 
        func_inps: Union[
            Dict[Hashable, Tuple[tuple,dict]],
            List[Tuple[tuple,dict]],
        ],
        data_dict: Dict[Hashable,Any],
        progress: Union[ProgressManager,None] = None,
    ):
        async with curio.TaskGroup() as task_group:
            for key, (args, kwargs) in get_kv_pairs(func_inps):
                task_group.spawn(func, key, data_dict, args, kwargs, progress)

async def asyncio_backend_coroutine(
    func: Callable, 
    func_inps: Union[
        Dict[Hashable, Tuple[tuple,dict]],
        List[Tuple[tuple,dict]],
    ],
    data_dict: Dict[Hashable,Any],
    progress: Union[ProgressManager,None] = None,
):
    tasks = []
    for key, (args, kwargs) in get_kv_pairs(func_inps):
        tasks.append(func(key, data_dict, args, kwargs, progress))
    await asyncio.gather(*tasks)
            
def run_coroutine(
    func: Callable,
    func_inps: Union[
        Dict[Hashable, Tuple[tuple,dict]],
        List[Tuple[tuple,dict]],
    ],
    data_dict: Optional[Dict[Hashable,Dict[int,Any]]] = None,
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
            ]
        ]],
    ] = 'trio',
    progress: Union[ProgressManager,bool] = True,
    description: str = None,
) -> Dict:
    need_close = False
    if data_dict is None:
        data_dict = {}
    if backend == 'trio':
        if not HAS_TRIO_BACKEND:
            raise ImportError('trio backend is not available')
        coroutine = trio_backend_coroutine
    elif backend == 'curio':
        if not HAS_CURIO_BACKEND:
            raise ImportError('curio backend is not available')
        coroutine = curio_backend_coroutine
    elif backend == 'asyncio':
        coroutine = asyncio_backend_coroutine
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
        progress.add_task(task_name=func.__inner_func_name__,total=len(func_inps),description=description)
    if backend == 'trio':
        if not HAS_TRIO_BACKEND:
            raise ImportError('trio backend is not available')
        trio.run(coroutine, func, func_inps, data_dict, progress)
    elif backend == 'curio':
        if not HAS_CURIO_BACKEND:
            raise ImportError('curio backend is not available')
        curio.run(coroutine, func, func_inps, data_dict, progress)
    elif backend == 'asyncio':
        asyncio.run(coroutine(func, func_inps, data_dict, progress))
    if need_close:
        progress.stop()
    return data_dict