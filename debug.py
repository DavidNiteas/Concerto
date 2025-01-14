from concerto.task_state_machine import (
    AbstractTask, 
    TaskConfig, 
    TaskStateMachine, 
    TaskStateMachinePool, 
    MAIN_TASK, 
    COROUTINE_WORKER, 
    THREAD_WORKER, 
    PROCESS_WORKER, 
    DICT,
)
from concerto import async_tools,thread_tools,process_tools
from concerto.base_tools import ProgressManager
import numpy as np
import trio
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from typing import List, Dict, Tuple, Optional, Union, Literal, Hashable, Callable, Sequence, Any, FrozenSet, Set, Pattern

def dot(x,y):
    return np.dot(x,y)

class DotTask(AbstractTask):
    
    name = 'task-dot'
    worker = staticmethod(dot)
    input_names = (('x','y'),{})
    output_names = ('_d',)
    cache_vars = ('x','y')
    task_type = MAIN_TASK
    worker_type = COROUTINE_WORKER
    # worker_type = THREAD_WORKER
    # worker_type = PROCESS_WORKER
    use_progress = True
    
async def norm_step_async(x:np.ndarray,key:str,norm_dict:Dict[str,float]):
    norm_dict[f'_{key}_norm'] = np.linalg.norm(x)

async def norm_async(data_dict: Dict[str,Any]) -> Dict[str,Any]:
    norm_dict = {}
    async with trio.open_nursery() as nursery:
        for key, value in data_dict.items():
            if isinstance(value, np.ndarray):
                nursery.start_soon(norm_step_async,value,key,norm_dict)
    return norm_dict

def norm_step(x:np.ndarray,key:str,norm_dict:Dict[str,float]):
    norm_dict[f'_{key}_norm'] = np.linalg.norm(x)

def norm(data_dict: Dict[str,Any]) -> Dict[str,Any]:
    norm_dict = {}
    for key, value in data_dict.items():
        if isinstance(value, np.ndarray):
            norm_step(value,key,norm_dict)
    return norm_dict
    
class NormTask(AbstractTask):
    
    name = 'task-norm'
    worker = staticmethod(norm)
    # worker = staticmethod(norm_async)
    input_names = DICT
    output_names = DICT
    task_type = MAIN_TASK
    worker_type = COROUTINE_WORKER
    # worker_type = THREAD_WORKER
    # worker_type = PROCESS_WORKER
    use_progress = True
    
def cosine(d,nx,ny):
    return np.dot(d,nx) / (nx*ny)
    
class CosineTask(AbstractTask):
    
    name = 'task-cosine'
    worker = staticmethod(cosine)
    input_names = (('_d','_x_norm','_y_norm'),{})
    output_names = ('cos',)
    task_type = MAIN_TASK
    worker_type = COROUTINE_WORKER
    # worker_type = THREAD_WORKER
    # worker_type = PROCESS_WORKER
    use_progress = True

class CosineTaskConfig(TaskConfig):
    
    nodes = [
        [DotTask(), NormTask()],
        [CosineTask()],
    ]
    
def test_machine():
    cosine_task = CosineTaskConfig()
    with ProgressManager() as progress:
        with ThreadPoolExecutor() as thread_pool:
            with ProcessPoolExecutor() as process_pool:
                task_wrapper = cosine_task.get_task_wrapper(
                    initial_datas={'x':np.array([1,2,3]), 'y':np.array([4,5,6])},
                    machine_id=f'test',
                    progress=progress,
                    thread_pool=thread_pool,
                    process_pool=process_pool,
                    async_query_interval=0.1,
                )
                machine = TaskStateMachine(**task_wrapper)
                machine.start_serially()
                # trio.run(machine.start_asynchronously)
    print(machine.Datas)
    print('Done.')
    
def test_machine_multi():
    cosine_task = CosineTaskConfig()
    machines = TaskStateMachine.start_multiple_machines(
        cosine_task,[{'x':np.array([1,2,3]), 'y':np.array([4,5,6])} for i in range(10000)],
    )
    print(machines[9999].Datas)
    print('Done.')
    
def test_pool():
    cosine_task = CosineTaskConfig()
    task_wrappers = []
    for i in range(10000):
        task_wrapper = cosine_task.get_task_wrapper(
            initial_datas={'x':np.array([1,2,3]), 'y':np.array([4,5,6])},
        )
        task_wrappers.append(task_wrapper)
    pool = TaskStateMachinePool(
        task_wrappers,
        name='test',
        thread_num=4,
        process_num=2,
    )
    pool.open()
    # pool.start_serially()
    # loader,monitor = pool.start_serially_by_thread()
    # loader,monitor = pool.start_asynchronously_by_thread()
    pool.start_asynchronously()
    pool.join()
    pool.close()
    for machine in pool.get_done_machines():
        pass
    print(machine.Datas)
    
if __name__ == '__main__':
    test_machine_multi()
    # test_machine()
    # test_pool()