from __future__ import annotations
import trio
import inspect
from rich.console import Console
from rich.progress import Progress, ProgressColumn, GetTimeCallable, TaskID
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
import warnings
from typing import List, Dict, Tuple, Optional, Union, Literal, Hashable, Callable, Sequence, Any, TypeVar

# runtime state
NOT_BEGIN = -1
RUNNING = 0
END = 1
# task type
MAIN_TASK = 0
SIDE_TASK = 1
# worker type
COROUTINE_WORKER = 0
THREAD_WORKER = 1
PROCESS_WORKER = 2
# special output name
DICT = 0

class BaseTask:
    
    def __init__(self):
        pass

class Task(BaseTask):
    
    legal_task_types = {MAIN_TASK,SIDE_TASK,None}
    legal_worker_types = {COROUTINE_WORKER,THREAD_WORKER,PROCESS_WORKER,None}
    
    def __init__(
        self,
        name: Hashable, # task name, should be unique in the machine pool
        worker: Callable, # work function, can be a callable or a async coroutine function
        input_names: Union[ # to tell the machine which input will be used from the machine.Datas.
            str, # use a single input value, which name is the input name
            Tuple[str,...], # use a tuple as input, every element is mapped to a input name
            None, # no input
        ],
        output_names: Union[ # to tell the machine which output will be returned to the machine.Datas.
            str, # use a single return value as output, which name is the output name
            Tuple[str], # use a tuple as output, every element is mapped to a output name
            Literal[0], # use a single dict as output, which key is the output name
            None, # no output
        ],
        *args,
        task_type: Union[
            Literal[
                0, # MAIN_TASK: if an error occurs in this task, the machine will stop and raise an exception.
                1, # SIDE_TASK: if an error occurs in this task, the machine will warn and save the error to the task.
            ],
            None, # if None, machine will decide the task type automatically.
        ] = None,
        worker_type: Union[
            Literal[
                0, # COROUTINE_WORKER: worker will working based on trio.
                1, # THREAD_WORKER: worker will working based on ThreadPoolExecutor.
                2, # PROCESS_WORKER: worker will working based on ProcessPoolExecutor.
            ],
            None, # if None, machine will decide the worker type automatically.
        ] = None,
        cache_vars: Optional[Tuple[str,...]] = None, # variables in this tuple will be deleted after the machine close.
        use_progress: Optional[bool] = None, # if True, worker will use rich.progress to show progress bar.
        keep_worker_silent: Optional[bool] = None, # if True, worker will not print any message to console.(Not use)
        **kwargs
    ):
        # base infomation
        self.name = name
        self.worker = worker
        self.input_names = input_names
        if isinstance(self.input_names, str):
            self.input_names = (self.input_names,)
        self.output_names = output_names
        if self.output_names != DICT and isinstance(self.output_names, str):
            self.output_names = (self.output_names,)
        # optional infomation
        self.task_type = task_type
        self.worker_type = worker_type
        self.cache_vars = cache_vars
        self.use_progress = use_progress
        self.keep_worker_silent = keep_worker_silent
        # runtime infomation
        self.state = NOT_BEGIN
        self.error = None
        self.validation_check()
        
    def validation_check(self) -> None:
        if self.TaskType not in self.legal_task_types:
            raise ValueError(f'TaskType should be one of {self.legal_task_types}, but task {self.name} got {self.task_type}.')
        if self.WorkerType not in self.legal_worker_types:
            raise ValueError(f'WorkerType should be one of {self.legal_worker_types}, but task {self.name} got {self.worker_type}.')
        if not callable(self.Worker):
            raise ValueError(f'Worker should be a callable, but task {self.name} got {self.worker.__class__.__name__}.')
        if not isinstance(self.InputNames, tuple) and self.InputNames is not None:
            raise ValueError(f'InputNames should be a tuple or None, but task {self.name} got {self.input_names.__class__.__name__}.')
        if self.OutputNames is not None and not isinstance(self.OutputNames, tuple) and self.OutputNames != DICT:
            raise ValueError(f'OutputNames should be a tuple or "Dict" or None, but task {self.name} got {self.output_names.__class__.__name__}.')
        
    # base infomation
    @property
    def Name(self) -> Hashable:
        return self.name
    
    @property
    def Worker(self) -> Callable:
        return self.worker
    
    @property
    def InputNames(self) -> Optional[Tuple[str,...]]:
        return self.input_names
    
    @property
    def OutputNames(self) -> Union[Tuple[str],Literal[0],None]:
        return self.output_names
    
    # optional infomation
    @property
    def TaskType(self) -> Union[Literal[0,1],None]:
        return self.task_type
    
    @TaskType.setter
    def TaskType(self, value: Union[Literal[0,1]]):
        self.task_type = value
    
    @property
    def WorkerType(self) -> Union[Literal[0,1,2],None]:
        return self.worker_type
    
    @WorkerType.setter
    def WorkerType(self, value: Union[Literal[0,1,2]]):
        self.worker_type = value
    
    @property
    def UseProgress(self) -> Optional[bool]:
        return self.use_progress
    
    @UseProgress.setter
    def UseProgress(self, value: bool):
        self.use_progress = value
    
    @property
    def KeepWorkerSilent(self) -> Optional[bool]:
        return self.keep_worker_silent
    
    @KeepWorkerSilent.setter
    def KeepWorkerSilent(self, value: bool):
        self.keep_worker_silent = value
        
    @property
    def CacheVars(self) -> Optional[Tuple[str,...]]:
        return self.cache_vars
    
    # runtime infomation
    @property
    def State(self) -> Literal[-1,0,1]:
        return self.state
    
    @State.setter
    def State(self, value: Literal[-1,0,1]):
        self.state = value
        
    @property
    def Error(self) -> Optional[Exception]:
        return self.error
    
    @Error.setter
    def Error(self, value: Optional[Exception]):
        self.error = value
    
class NotBeginTask(BaseTask):
    
    @property
    def State(self) -> Literal[1]:
        return 1
    
    @property
    def is_all_done(self) -> bool:
        return True
    
class EndTask(BaseTask):
    
    @property
    def State(self) -> Literal[1]:
        return 1
    
    @property
    def is_all_done(self) -> bool:
        return True

class TaskGroup(BaseTask):
    
    def __init__(
        self,
        tasks: Union[Sequence[Task],Task],
    ):
        self.tasks = {}
        self.cache_vars = set()
        if isinstance(tasks, Task):
            tasks = [tasks]
        for task in tasks:
            self.tasks[task.Name] = task
            if task.CacheVars is not None:
                self.cache_vars.update(task.CacheVars)
        self.cache_vars = frozenset(self.cache_vars)
        
    def __len__(self) -> int:
        return len(self.Tasks)
    
    @property
    def Tasks(self) -> Dict[Hashable,Task]:
        return self.tasks
    
    @property
    def CacheVars(self) -> frozenset:
        return self.cache_vars
    
    @property
    def is_all_done(self) -> bool:
        return all(map(lambda task: task.State == END, self.Tasks.values()))
    
    @property
    def is_all_running(self) -> bool:
        return all(map(lambda task: task.State != NOT_BEGIN, self.Tasks.values()))
    
    @classmethod
    def from_list(cls, state_dict_list: List[dict]) -> TaskGroup:
        return cls([Task(**state_dict) for state_dict in state_dict_list])
    
    @classmethod
    def from_dict(cls, state_dict: Dict[Hashable,list]) -> TaskGroup:
        state_list = []
        for i, (attr_name, attr_list) in enumerate(state_dict.items()):
            while len(attr_list) > len(state_list):
                state_list.append({})
            state_list[i][attr_name] = attr_list[i]
        return cls.from_list(state_list)

class TaskMap():
    
    def __init__(
        self,
        nodes: List[TaskGroup],
    ):
        self.nodes = nodes
        self.pointer = -1
        self.cache_vars = set()
        for node in self.nodes:
            self.cache_vars.update(node.CacheVars)
        self.cache_vars = frozenset(self.cache_vars)
        
    @property
    def Nodes(self) -> List[TaskGroup]:
        return self.nodes
    
    @property
    def CacheVars(self) -> frozenset:
        return self.cache_vars
        
    def __len__(self) -> int:
        return len(self.Nodes)
    
    @property
    def now(self) -> Union[TaskGroup,EndTask,NotBeginTask]:
        if self.pointer >= len(self):
            return EndTask()
        elif self.pointer < 0:
            return NotBeginTask()
        else:
            return self.nodes[self.pointer]
        
    def __next__(self) -> Union[TaskGroup,EndTask]:
        now = self.now
        if now.is_all_done:
            if self.pointer < len(self):
                self.pointer += 1
            return self.now
        else:
            return now
    
    @classmethod
    def from_list(cls, nodes_list: List[List[dict]]) -> TaskMap:
        return cls([TaskGroup.from_list(node) for node in nodes_list])
    
    @classmethod
    def from_dict(cls, nodes_list: List[Dict[Hashable,list]]) -> TaskMap:
        return cls([TaskGroup.from_dict(node) for node in nodes_list])
    
class TaskProgressManager(Progress):
    
    def __init__(
        self,
        *columns: Union[str, ProgressColumn],
        console: Optional[Console] = None,
        auto_refresh: bool = True,
        refresh_per_second: float = 10,
        speed_estimate_period: float = 30.0,
        transient: bool = False,
        redirect_stdout: bool = True,
        redirect_stderr: bool = True,
        get_time: Optional[GetTimeCallable] = None,
        disable: bool = False,
        expand: bool = False,
    ):
        super().__init__(
            *columns,
            console=console,
            auto_refresh=auto_refresh,
            refresh_per_second=refresh_per_second,
            speed_estimate_period=speed_estimate_period,
            transient=transient,
            redirect_stdout=redirect_stdout,
            redirect_stderr=redirect_stderr,
            get_time=get_time,
            disable=disable,
            expand=expand,
        )
        self.task_name_id_map = {}
        
    @property
    def Name2ID(self) -> Dict[Hashable,TaskID]:
        return self.task_name_id_map

    def add_task(
        self,
        task_name: Hashable,
        description: Optional[str] = None,
        start: bool = True,
        total: Optional[float] = None,
        completed: int = 0,
        visible: bool = True,
        **fields: Any,
    ) -> None:
        if description is None:
            description = f'{str(task_name)}:'
        task_id = super().add_task(
            description,
            start=start,
            total=total,
            completed=completed,
            visible=visible,
            **fields,
        )
        self.Name2ID[task_name] = task_id
        
    def update(
        self,
        task_name: Hashable,
        *,
        total: Optional[float] = None,
        completed: Optional[float] = None,
        advance: Optional[float] = None,
        description: Optional[str] = None,
        visible: Optional[bool] = None,
        refresh: bool = False,
        **fields: Any,
    ) -> None:
        if task_name not in self.Name2ID:
            self.add_task(task_name)
        task_id = self.Name2ID[task_name]
        super().update(
            task_id,
            total=total,
            completed=completed,
            advance=advance,
            description=description,
            visible=visible,
            refresh=refresh,
            **fields,
        )

class TaskStateMachine():
    
    defualt_task_prameters = {
        'task_type': MAIN_TASK,
        'worker_type': COROUTINE_WORKER,
        'use_progress': True,
        'keep_worker_silent': False,
    }
    
    def __init__(
        self,
        machine_id: Hashable,
        task_map: TaskMap,
        initial_dates: Dict[Hashable,Any],
        thread_pool: Optional[ThreadPoolExecutor] = None,
        process_pool: Optional[ProcessPoolExecutor] = None,
        progress: Optional[TaskProgressManager] = None,
        defualt_task_prameters: Optional[Dict[str,Any]] = None,
    ):
        self.machine_id = machine_id
        self.task_map = task_map
        self.datas = initial_dates
        if defualt_task_prameters is None:
            self.defualt_task_prameters = {**self.defualt_task_prameters, **self.defualt_task_prameters}
        self.open(thread_pool, process_pool, progress)
        
    def open(
        self,
        thread_pool: Optional[ThreadPoolExecutor] = None,
        process_pool: Optional[ProcessPoolExecutor] = None,
        progress: Optional[TaskProgressManager] = None,
    ) -> None:
        self.thread_pool = thread_pool
        self.process_pool = process_pool
        self.progress = progress
        self.State = NOT_BEGIN
        
    def close(self) -> None:
        self.thread_pool = None
        self.process_pool = None
        self.progress = None
        self.State = END
        for cache_name in self.task_map.CacheVars:
            if cache_name in self.datas:
                del self.datas[cache_name]
    
    @property
    def ID(self) -> Hashable:
        return self.machine_id
    
    @property
    def TaskMap(self) -> TaskMap:
        return self.task_map
    
    @property
    def Datas(self) -> Dict[Hashable,Any]:
        return self.datas
    
    @property
    def State(self) -> int:
        return self.state
    
    @State.setter
    def State(self, value: int):
        self.state = value
        
    @property
    def ThreadPool(self) -> Optional[ThreadPoolExecutor]:
        return self.thread_pool
    
    @property
    def ProcessPool(self) -> Optional[ProcessPoolExecutor]:
        return self.process_pool
    
    @property
    def Progress(self) -> Optional[TaskProgressManager]:
        return self.progress
    
    # ↓↓↓↓↓ Synchronous Methods ↓↓↓↓↓
    
    def init_task_parameters(
        self,
        task: Task,
    ) -> None:
        for attr_name, attr_value in self.defualt_task_prameters.items():
            if getattr(task, attr_name) is None:
                setattr(task, attr_name, attr_value)
            
    def init_worker_input(
        self,
        task: Task,
    ) -> Dict[str,Any]:
        input_dict = {}
        if task.InputNames is not None:
            for input_name in task.InputNames:
                if input_name not in self.Datas:
                    raise ValueError(f'Task:{task.Name} require input:{input_name} but it is not found in machine:{self.ID}')
                input_dict[input_name] = self.Datas[input_name]
        return input_dict
    
    def start_worker(
        self,
        task: Task,
        input_dict: Dict[str,Any],
    ) -> Union[Tuple[Any,...],Dict[str,Any],Any]:
        if task.WorkerType == COROUTINE_WORKER:
            if inspect.iscoroutinefunction(task.Worker):
                raise RuntimeError(f'The machine:{self.ID} is running in synchronous mode, but task:{task.Name} has a coroutine worker')
            return task.Worker(**input_dict)
        elif task.WorkerType == THREAD_WORKER:
            if self.ThreadPool is None:
                raise ValueError(f'Task:{task.Name} in Machine:{self.ID} require thread worker but thread pool is not set')
            future = self.ThreadPool.submit(task.Worker, **input_dict)
            return future.result()
        elif task.WorkerType == PROCESS_WORKER:
            if self.ProcessPool is None:
                raise ValueError(f'Task:{task.Name} in Machine:{self.ID} require process worker but process pool is not set')
            future = self.ProcessPool.submit(task.Worker, **input_dict)
            return future.result()
        else:
            raise ValueError(f'Task:{task.Name} in Machine:{self.ID} has unknown worker type:{task.WorkerType}')
    
    def update_machine_data(
        self,
        worker_output: Union[Tuple[Any,...],Dict[str,Any],Any,None],
        task: Task,
    ) -> None:
        if task.OutputNames is None:
            pass
        elif task.OutputNames == DICT:
            if not isinstance(worker_output, dict):
                raise ValueError(f'Task:{task.Name} output should be a dict but it is not')
            self.Datas.update(worker_output)
        elif isinstance(task.OutputNames, tuple):
            if not isinstance(worker_output, tuple) and len(task.OutputNames) > 1:
                raise ValueError(f'Task:{task.Name} output should be a tuple but it is not')
            elif len(task.OutputNames) == 1 and not isinstance(worker_output, tuple):
                worker_output = (worker_output,)
            if len(worker_output) != len(task.OutputNames):
                raise ValueError(f'Task:{task.Name} output should have {len(task.OutputNames)} elements but it has {len(worker_output)}')
            for output_name, output_value in zip(task.OutputNames, worker_output):
                if not isinstance(output_name, str) and output_name is not None:
                    raise ValueError(f'Task:{task.Name} output name should be a str or None but it is {type(output_name)}')
                if output_name is not None:
                    self.Datas[output_name] = output_value
                    
    def run_task(
        self,
        task: Task,
    ) -> None:
        task.State = RUNNING
        try:
            self.init_task_parameters(task)
            if task.Name not in self.Progress.Name2ID:
                self.Progress.add_task(task.Name,visible=task.UseProgress)
            input_dict = self.init_worker_input(task)
            worker_output = self.start_worker(task, input_dict)
            self.update_machine_data(worker_output, task)
        except Exception as e:
            if task.TaskType != MAIN_TASK:
                task.Error = e
                warnings.warn(f'Task:{task.Name} in Machine:{self.ID} failed with error:{e}')
            else:
                raise e
        self.Progress.update(task.Name, advance=1, visible=task.UseProgress)
        task.State = END

    def start_serially(self):
        self.State = RUNNING
        while self.State == RUNNING:
            task_group = next(self.task_map)
            if isinstance(task_group, EndTask):
                break
            elif isinstance(task_group, NotBeginTask):
                continue
            else:
                for task in task_group.Tasks.values():
                    if task.State == NOT_BEGIN:
                        self.run_task(task)
        self.close()
        
    # ↓↓↓↓↓ Asynchronous Methods ↓↓↓↓↓
    
    async def start_worker_async(
        self,
        task: Task,
        input_dict: Dict[str,Any],
    ) -> Union[Tuple[Any,...],Dict[str,Any],Any]:
        if task.WorkerType == COROUTINE_WORKER:
            if inspect.iscoroutinefunction(task.Worker):
                return await task.Worker(**input_dict)
            else:
                return task.Worker(**input_dict)
        elif task.WorkerType == THREAD_WORKER:
            if self.ThreadPool is None:
                raise ValueError(f'Task:{task.Name} in Machine:{self.ID} require thread worker but thread pool is not set')
            future = self.ThreadPool.submit(task.Worker, **input_dict)
            return future.result()
        elif task.WorkerType == PROCESS_WORKER:
            if self.ProcessPool is None:
                raise ValueError(f'Task:{task.Name} in Machine:{self.ID} require process worker but process pool is not set')
            future = self.ProcessPool.submit(task.Worker, **input_dict)
            return future.result()
        else:
            raise ValueError(f'Task:{task.Name} in Machine:{self.ID} has unknown worker type:{task.WorkerType}')
        
    async def run_task_async(
        self,
        task: Task,
    ) -> None:
        task.State = RUNNING
        try:
            self.init_task_parameters(task)
            if task.Name not in self.Progress.Name2ID:
                self.Progress.add_task(task.Name, visible=task.UseProgress)
            input_dict = self.init_worker_input(task)
            worker_output = await self.start_worker_async(task, input_dict)
            self.update_machine_data(worker_output, task)
        except Exception as e:
            if task.TaskType != MAIN_TASK:
                task.Error = e
                warnings.warn(f'Task:{task.Name} in Machine:{self.ID} failed with error:{e}')
            else:
                raise e
        self.Progress.update(task.Name, advance=1, visible=task.UseProgress)
        task.State = END
        
    async def start_asynchronously(self):
        self.State = RUNNING
        while self.State == RUNNING:
            task_group = next(self.task_map)
            if isinstance(task_group, EndTask):
                break
            elif isinstance(task_group, NotBeginTask):
                continue
            else:
                if not task_group.is_all_running:
                    async with trio.open_nursery() as nursery:
                        for task in task_group.Tasks.values():
                            if task.State == NOT_BEGIN:
                                nursery.start_soon(self.run_task_async, task)
                                task.State = RUNNING
        self.close()
        
# test code
import numpy as np
def dot(x,y):
    return np.dot(x,y)
def norm_x(x):
    return np.linalg.norm(x)
def norm_y(y):
    return np.linalg.norm(y)
def cosine(d,nx,ny):
    return np.dot(d,nx) / (nx*ny)
        
if __name__ == '__main__':
    task_dot = {
        'name': 'dot',
        'worker': dot,
        'input_names': ('x','y'),
        'output_names': 'd',
        'task_type': MAIN_TASK,
        'worker_type': COROUTINE_WORKER,
        'cache_vars': ('x','y','d'),
        'use_progress': False,
    }
    task_norm_x = {
        'name': 'norm_x',
        'worker': norm_x,
        'input_names': ('x'),
        'output_names': 'nx',
        'task_type': MAIN_TASK,
        'worker_type': THREAD_WORKER,
        'cache_vars': ('x','nx'),
        'use_progress': False,
    }
    task_norm_y = {
        'name': 'norm_y',
        'worker': norm_y,
        'input_names': ('y'),
        'output_names': 'ny',
        'task_type': MAIN_TASK,
        'worker_type': PROCESS_WORKER,
        'cache_vars': ('y','ny'),
        'use_progress': False,
    }
    task_cosine = {
        'name': 'cosine',
        'worker': cosine,
        'input_names': ('d','nx','ny'),
        'output_names': 'cos',
        # 'task_type': MAIN_TASK,
        # 'worker_type': COROUTINE_WORKER,
        'use_progress': False,
    }
    cosin_node = [
        [task_dot, task_norm_x, task_norm_y],
        [task_cosine],
    ]
    task_map = TaskMap.from_list(cosin_node)
    with TaskProgressManager() as progress:
        with ThreadPoolExecutor() as thread_pool:
            with ProcessPoolExecutor() as process_pool:
                machine = TaskStateMachine(
                    machine_id='test',
                    task_map=task_map,
                    initial_dates={'x':np.array([1,2,3]), 'y':np.array([4,5,6])},
                    progress=progress,
                    thread_pool=thread_pool,
                    process_pool=process_pool,
                )
                # machine.start_serially()
                trio.run(machine.start_asynchronously)
    print(machine.Datas)
    task_map