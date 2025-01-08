from __future__ import annotations
import trio
import queue
import inspect
from rich.console import Console
from rich.progress import Progress, ProgressColumn, GetTimeCallable, TaskID
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
import warnings
from typing import List, Dict, Tuple, Optional, Union, Literal, Hashable, Callable, Sequence, Any, FrozenSet, Set, Pattern

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
# special i/o name
DICT = 0
NULL = 1

class BaseTask:
    
    legal_task_types_literal  = {MAIN_TASK,SIDE_TASK,None}
    legal_worker_types_literal  = {COROUTINE_WORKER,THREAD_WORKER,PROCESS_WORKER,None}
    legal_input_names_literal = {None,DICT}
    legal_output_names_literal  = {None,DICT}
    
    def __init__(self):
        self.name = None
        self.worker = None
        self.input_names = None
        self.output_names = None
        self.task_type = None
        self.worker_type = None
        self.cache_vars = None
        self.use_progress = None
        self.keep_worker_silent = None
    
    @property
    def Datas(self) -> BaseTask:
        return self
    
    # base infomation
    @property
    def Name(self) -> Hashable:
        return self.Datas.name
    
    @property
    def Worker(self) -> Callable:
        return self.Datas.worker
    
    @property
    def InputNames(self) -> Union[
        Tuple[
            Tuple[str,...], # args
            Dict[str,str], # kwargs
        ],
        Literal[0],
        None,
        Callable[
            [Dict[str,Any]], # machine.Datas
            Tuple[
                Tuple[Any,...], # args
                Dict[str,Any], # kwargs
            ]
        ]
    ]:
        return self.Datas.input_names
    
    @property
    def OutputNames(self) -> Union[
        Tuple[str],
        Literal[0],
        None,
        Callable[
            [
                Dict[str,Any], # machine.Datas
                Any # output value
            ],
            None,
        ]
    ]:
        return self.Datas.output_names
    
    # optional infomation
    @property
    def TaskType(self) -> Union[Literal[0,1],None]:
        return self.Datas.task_type
    
    @TaskType.setter
    def TaskType(self, value: Union[Literal[0,1]]):
        self.Datas.task_type = value
    
    @property
    def WorkerType(self) -> Union[Literal[0,1,2],None]:
        return self.Datas.worker_type
    
    @WorkerType.setter
    def WorkerType(self, value: Union[Literal[0,1,2]]):
        self.Datas.worker_type = value
    
    @property
    def UseProgress(self) -> Optional[bool]:
        return self.Datas.use_progress
    
    @UseProgress.setter
    def UseProgress(self, value: bool):
        self.Datas.use_progress = value
    
    @property
    def KeepWorkerSilent(self) -> Optional[bool]:
        return self.Datas.keep_worker_silent
    
    @KeepWorkerSilent.setter
    def KeepWorkerSilent(self, value: bool):
        self.Datas.keep_worker_silent = value
        
    @property
    def CacheVars(self) -> Optional[Tuple[str,...]]:
        return self.Datas.cache_vars
    
class AbstractTask(BaseTask):
    
    name = None
    worker = None
    input_names = None
    output_names = None
    task_type = None
    worker_type = None
    cache_vars = None
    use_progress = None
    keep_worker_silent = None
    
    def __init__(
        self,
        name: Hashable = None, # task name, should be unique in the machine pool
        worker: Callable = None, # work function, can be a callable or a async coroutine function
        input_names: Union[ # to tell the machine which input will be used from the machine.Datas.
            Tuple[ # define the structure of input args and kwargs.
                Tuple[str,...], # input args, machine will search the corresponding value in machine.Datas.
                Dict[ # input kwargs
                    str, # the key of machine.Datas, machine will search the corresponding value in machine.Datas.
                    str, # the parameter name of the worker.
                ], 
            ],
            Literal[0], # use machine.Datas as single input (In this mode, if a multiprocessing backend is used, it may incur additional performance overhead due to serialization.)
            Literal[1], # no input
            Callable[[Dict[str,Any]],Dict[str,Any]] # use a function convert machine.Datas to input dict.
        ] = None,
        output_names: Union[ # to tell the machine which output will be returned to the machine.Datas.
            Tuple[str,...], # use a tuple as output, every element is mapped to a output name
            Literal[0], # use a single dict as output, which key is the output name
            Literal[1], # no output
            Callable[[Dict[str,Any],Any],None] # use a function to save output to machine.Datas.
        ] = None,
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
        if name is not None:
            self.name = name
        if worker is not None:
            self.worker = worker
        if input_names is not None:
            if isinstance(input_names, str):
                input_names = ((input_names,),{})
            self.input_names = input_names
        if output_names is not None:
            if isinstance(output_names, str):
                output_names = ((output_names,),{})
            self.output_names = output_names
        # optional infomation
        if task_type is not None:
            self.task_type = task_type
        if worker_type is not None:
            self.worker_type = worker_type
        if cache_vars is not None:
            self.cache_vars = cache_vars
        if use_progress is not None:
            self.use_progress = use_progress
        if keep_worker_silent is not None:
            self.keep_worker_silent = keep_worker_silent
        # validation check
        self.validation_check()
        
    def validation_check(self) -> None:
        if self.TaskType not in self.legal_task_types_literal:
            raise ValueError(f'TaskType should be one of {self.legal_task_types_literal}, but task {self.name} got {self.task_type}.')
        if self.WorkerType not in self.legal_worker_types_literal:
            raise ValueError(f'WorkerType should be one of {self.legal_worker_types_literal}, but task {self.name} got {self.worker_type}.')
        if not callable(self.Worker):
            raise ValueError(f'Worker should be a callable, but task {self.name} got {self.worker.__class__.__name__}.')
        if not isinstance(self.InputNames, tuple) and self.InputNames not in self.legal_input_names_literal:
            raise ValueError(f'InputNames should be a tuple or Literal[{self.legal_input_names_literal}], but task {self.name} got {self.input_names}.')
        if not isinstance(self.OutputNames, tuple) and self.OutputNames not in self.legal_output_names_literal:
            raise ValueError(f'OutputNames should be a tuple or Literal[{self.legal_output_names_literal}], but task {self.name} got {self.output_names}.')

class Task(BaseTask):
    
    def __init__(
        self,
        abstract_task: AbstractTask,
    ):
        self.abstract_task = abstract_task
        # runtime infomation
        self.state = NOT_BEGIN
        self.error = None
        
    @property
    def Datas(self) -> AbstractTask:
        return self.abstract_task
    
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
    def CacheVars(self) -> FrozenSet[str]:
        return self.cache_vars
    
    @property
    def is_all_done(self) -> bool:
        return all(map(lambda task: task.State == END, self.Tasks.values()))
    
    @property
    def is_all_running(self) -> bool:
        return all(map(lambda task: task.State != NOT_BEGIN, self.Tasks.values()))
    
    @classmethod
    def from_AbstractTask(cls, abstract_task_list: List[AbstractTask]) -> TaskGroup:
        return cls([Task(abstract_task) for abstract_task in abstract_task_list])

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
    def CacheVars(self) -> FrozenSet[str]:
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
    def from_AbstractTask(cls, nodes_list: List[List[AbstractTask]]) -> TaskMap:
        return cls([TaskGroup.from_AbstractTask(node) for node in nodes_list])
    
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
        
    def update_total(
        self,
        task_name: Hashable,
        advance: float = 1,
        completed: Optional[float] = None,
    ) -> None:
        if task_name in self.Name2ID:
            if completed is None:
                self._tasks[self.Name2ID[task_name]].total += advance
            else:
                self._tasks[self.Name2ID[task_name]].total = completed

class TaskStateMachine():
    
    defualt_task_prameters = {
        'TaskType': MAIN_TASK,
        'WorkerType': COROUTINE_WORKER,
        'UseProgress': True,
        'KeepWorkerSilent': False,
    }
    async_query_interval: float = 0.1
    
    @classmethod
    def from_task_wrapper(cls,wrapper:TaskWrapper) -> TaskStateMachine:
        return cls(**wrapper)
    
    def __init__(
        self,
        machine_id: Hashable,
        task_map: TaskMap,
        initial_datas: Dict[str,Any],
        thread_pool: Optional[ThreadPoolExecutor] = None,
        process_pool: Optional[ProcessPoolExecutor] = None,
        progress: Optional[TaskProgressManager] = None,
        defualt_task_prameters: Optional[Dict[str,Any]] = None,
        async_query_interval: Optional[float] = None,
    ):
        self.machine_id = machine_id
        self.task_map = task_map
        self.datas = initial_datas
        if defualt_task_prameters is not None:
            self.defualt_task_prameters = {**self.defualt_task_prameters, **defualt_task_prameters}
        if async_query_interval is not None:
            self.async_query_interval = async_query_interval
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
        for task_group in self.TaskMap.nodes:
            for task in task_group.Tasks.values():
                self.add_task_to_progress(task)
        
    def close(self) -> None:
        self.thread_pool = None
        self.process_pool = None
        self.progress = None
        self.State = END
        cache_names = set()
        for data_name in self.Datas.keys():
            if data_name in self.task_map.CacheVars or data_name.startswith('_'):
                cache_names.add(data_name)
        for cache_name in cache_names:
            if cache_name in self.datas:
                del self.datas[cache_name]
    
    @property
    def ID(self) -> Hashable:
        return self.machine_id
    
    @property
    def TaskMap(self) -> TaskMap:
        return self.task_map
    
    @property
    def Datas(self) -> Dict[str,Any]:
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
    
    def add_task_to_progress(
        self,
        task: Task,
    ) -> None:
        if self.Progress is not None:
            if task.Name not in self.Progress.Name2ID:
                self.Progress.add_task(task.Name,visible=task.UseProgress,total=1)
            else:
                self.Progress.update_total(task.Name)
                
    def update_task_progress(
        self,
        task: Task,
        advance: float = 1,
    ) -> None:
        if self.Progress is not None:
            self.Progress.update(task.Name, advance=advance, visible=task.UseProgress)
    
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
    ) -> Tuple[
        Tuple[Any,...], # input args
        Dict[str,Any], # input kwargs
    ]:
        args = []
        kwargs = {}
        if task.InputNames is not None:
            if isinstance(task.InputNames, tuple):
                for input_name in task.InputNames[0]:
                    if input_name not in self.Datas:
                        warnings.warn(f'Task:{task.Name} require input:{input_name} but it is not found in machine:{self.ID}, we will use None as input')
                        args.append(None)
                    else:
                        args.append(self.Datas[input_name])
                for input_name, worker_input_name in task.InputNames[1].items():
                    if input_name not in self.Datas:
                        warnings.warn(f'Task:{task.Name} require input:{input_name} but it is not found in machine:{self.ID}, we will use None as input')
                        kwargs[worker_input_name] = None
                    else:
                        kwargs[worker_input_name] = self.Datas[input_name]
            elif task.InputNames == DICT:
                args.append(self.Datas)
            elif callable(task.InputNames):
                if inspect.iscoroutinefunction(task.InputNames):
                    raise ValueError(f'The machine:{self.ID} is running in synchronous mode, but task:{task.Name} has a coroutine input process function')
                args, kwargs = task.InputNames(self.Datas)
            else:
                raise ValueError(f'Task:{task.Name} has unknown input names type:{task.InputNames}')
        return args, kwargs
    
    def start_worker(
        self,
        task: Task,
        args: Tuple[Any,...],
        kwargs: Dict[str,Any],
    ) -> Union[Tuple[Any,...],Dict[str,Any],Any]:
        if task.WorkerType == COROUTINE_WORKER:
            if inspect.iscoroutinefunction(task.Worker):
                raise RuntimeError(f'The machine:{self.ID} is running in synchronous mode, but task:{task.Name} has a coroutine worker')
            return task.Worker(*args,**kwargs)
        elif task.WorkerType == THREAD_WORKER:
            if self.ThreadPool is None:
                raise ValueError(f'Task:{task.Name} in Machine:{self.ID} require thread worker but thread pool is not set')
            future = self.ThreadPool.submit(task.Worker, *args,**kwargs)
            return future.result()
        elif task.WorkerType == PROCESS_WORKER:
            if self.ProcessPool is None:
                raise ValueError(f'Task:{task.Name} in Machine:{self.ID} require process worker but process pool is not set')
            future = self.ProcessPool.submit(task.Worker, *args,**kwargs)
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
        elif callable(task.OutputNames):
            if inspect.iscoroutinefunction(task.OutputNames):
                raise ValueError(f'The machine:{self.ID} is running in synchronous mode, but task:{task.Name} has a coroutine output process function')
            task.OutputNames(self.Datas, worker_output)
        else:
            raise ValueError(f'Task:{task.Name} has unknown output names type:{task.OutputNames}')
                    
    def run_task(
        self,
        task: Task,
    ) -> None:
        task.State = RUNNING
        try:
            self.init_task_parameters(task)
            input_args, input_kwargs = self.init_worker_input(task)
            worker_output = self.start_worker(task, input_args, input_kwargs)
            self.update_machine_data(worker_output, task)
        except Exception as e:
            if task.TaskType != MAIN_TASK:
                task.Error = e
                warnings.warn(f'Task:{task.Name} in Machine:{self.ID} failed with error:{e}')
            else:
                raise e
        self.update_task_progress(task)
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
    
    async def init_worker_input_async(
        self,
        task: Task,
    ) -> Tuple[
        Tuple[Any,...], # input args
        Dict[str,Any], # input kwargs
    ]:
        args = []
        kwargs = {}
        if task.InputNames is not None:
            if isinstance(task.InputNames, tuple):
                for input_name in task.InputNames[0]:
                    if input_name not in self.Datas:
                        warnings.warn(f'Task:{task.Name} require input:{input_name} but it is not found in machine:{self.ID}, we will use None as input')
                        args.append(None)
                    else:
                        args.append(self.Datas[input_name])
                for input_name, worker_input_name in task.InputNames[1].items():
                    if input_name not in self.Datas:
                        warnings.warn(f'Task:{task.Name} require input:{input_name} but it is not found in machine:{self.ID}, we will use None as input')
                        kwargs[worker_input_name] = None
                    else:
                        kwargs[worker_input_name] = self.Datas[input_name]
            elif task.InputNames == DICT:
                args.append(self.Datas)
            elif callable(task.InputNames):
                if inspect.iscoroutinefunction(task.InputNames):
                    args, kwargs = await task.InputNames(self.Datas)
                else:
                    args, kwargs = task.InputNames(self.Datas)
            else:
                raise ValueError(f'Task:{task.Name} has unknown input names type:{task.InputNames}')
        return args, kwargs
    
    async def start_worker_async(
        self,
        task: Task,
        args: Tuple[Any,...],
        kwargs: Dict[str,Any],
    ) -> Union[Tuple[Any,...],Dict[str,Any],Any]:
        if task.WorkerType == COROUTINE_WORKER:
            if inspect.iscoroutinefunction(task.Worker):
                return await task.Worker(*args,**kwargs)
            else:
                return task.Worker(*args,**kwargs)
        elif task.WorkerType == THREAD_WORKER:
            if self.ThreadPool is None:
                raise ValueError(f'Task:{task.Name} in Machine:{self.ID} require thread worker but thread pool is not set')
            future = self.ThreadPool.submit(task.Worker, *args,**kwargs)
            return future.result()
        elif task.WorkerType == PROCESS_WORKER:
            if self.ProcessPool is None:
                raise ValueError(f'Task:{task.Name} in Machine:{self.ID} require process worker but process pool is not set')
            future = self.ProcessPool.submit(task.Worker, *args,**kwargs)
            return future.result()
        else:
            raise ValueError(f'Task:{task.Name} in Machine:{self.ID} has unknown worker type:{task.WorkerType}')
        
    async def update_machine_data_async(
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
        elif callable(task.OutputNames):
            if inspect.iscoroutinefunction(task.OutputNames):
                await task.OutputNames(self.Datas, worker_output)
            else:
                task.OutputNames(self.Datas, worker_output)
        else:
            raise ValueError(f'Task:{task.Name} has unknown output names type:{task.OutputNames}')

    async def run_task_async(
        self,
        task: Task,
    ) -> None:
        task.State = RUNNING
        try:
            self.init_task_parameters(task)
            input_args, input_kwargs = await self.init_worker_input_async(task)
            worker_output = await self.start_worker_async(task, input_args, input_kwargs)
            await self.update_machine_data_async(worker_output, task)
        except Exception as e:
            if task.TaskType != MAIN_TASK:
                task.Error = e
                warnings.warn(f'Task:{task.Name} in Machine:{self.ID} failed with error:{e}')
            else:
                raise e
        self.update_task_progress(task)
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
            trio.sleep(self.async_query_interval)
        self.close()
        
class TaskWrapper(dict):
    
    def __init__(
        self,
        machine_id: Hashable,
        task_map: TaskMap,
        initial_datas: Dict[str,Any],
        thread_pool: Optional[ThreadPoolExecutor] = None,
        process_pool: Optional[ProcessPoolExecutor] = None,
        progress: Optional[TaskProgressManager] = None,
        defualt_task_prameters: Optional[Dict[str,Any]] = None,
        async_query_interval: float = 0.1,
        **kwargs,
    ):
        super().__init__(
            machine_id=machine_id,
            task_map=task_map,
            initial_datas=initial_datas,
            thread_pool=thread_pool,
            process_pool=process_pool,
            progress=progress,
            defualt_task_prameters=defualt_task_prameters,
            async_query_interval=async_query_interval,
            **kwargs,
        )
        
    @property
    def MachineID(self) -> Hashable:
        return self['machine_id']
    
    @MachineID.setter
    def MachineID(self, value: Hashable):
        self['machine_id'] = value
        
    @property
    def TaskMap(self) -> TaskMap:
        return self['task_map']
    
    @TaskMap.setter
    def TaskMap(self, value: TaskMap):
        self['task_map'] = value
        
    @property
    def InitialDates(self) -> Dict[Hashable,Any]:
        return self['initial_datas']
    
    @InitialDates.setter
    def InitialDates(self, value: Dict[Hashable,Any]):
        self['initial_datas'] = value
        
    @property
    def ThreadPool(self) -> Optional[ThreadPoolExecutor]:
        return self['thread_pool']
    
    @ThreadPool.setter
    def ThreadPool(self, value: Optional[ThreadPoolExecutor]):
        self['thread_pool'] = value
        
    @property
    def ProcessPool(self) -> Optional[ProcessPoolExecutor]:
        return self['process_pool']
    
    @ProcessPool.setter
    def ProcessPool(self, value: Optional[ProcessPoolExecutor]):
        self['process_pool'] = value
        
    @property
    def Progress(self) -> Optional[TaskProgressManager]:
        return self['progress']
    
    @Progress.setter
    def Progress(self, value: Optional[TaskProgressManager]):
        self['progress'] = value
        
    @property
    def DefualtTaskPrameters(self) -> Optional[Dict[str,Any]]:
        return self['defualt_task_prameters']
    
    @DefualtTaskPrameters.setter
    def DefualtTaskPrameters(self, value: Optional[Dict[str,Any]]):
        self['defualt_task_prameters'] = value
        
    @property
    def AsyncQueryInterval(self) -> float:
        return self['async_query_interval']
    
    @AsyncQueryInterval.setter
    def AsyncQueryInterval(self, value: float):
        self['async_query_interval'] = value
        
class TaskConfig():
    
    nodes: List[List[AbstractTask]] = None
    defualt_task_prameters: Dict[str,Any] = None
    async_query_interval: float = None
    defualt_initial_datas: Dict[str,Any] = {}
    
    def __init__(
        self,
        nodes: Optional[List[List[AbstractTask]]] = None,
        defualt_initial_datas: Optional[Dict[str,Any]] = None,
    ) -> None:
        if nodes is not None:
            self.nodes = nodes
        if defualt_initial_datas is not None:
            self.defualt_initial_datas = {**self.defualt_initial_datas, **defualt_initial_datas}
        
    def get_task_wrapper(
        self,
        machine_id: Hashable,
        initial_datas: Dict[str,Any],
        progress: Optional[TaskProgressManager] = None,
        thread_pool: Optional[ThreadPoolExecutor] = None,
        process_pool: Optional[ProcessPoolExecutor] = None,
        defualt_task_prameters: Optional[Dict[str,Any]] = None,
        async_query_interval: Optional[float] = None,
    ) -> TaskWrapper:
        if async_query_interval is None:
            async_query_interval = self.async_query_interval
        if defualt_task_prameters is None:
            defualt_task_prameters = self.defualt_task_prameters
        initial_datas = {**self.defualt_initial_datas, **initial_datas}
        return TaskWrapper(
            machine_id=machine_id,
            task_map=TaskMap.from_AbstractTask(self.nodes),
            initial_datas=initial_datas,
            progress=progress,
            thread_pool=thread_pool,
            process_pool=process_pool,
            defualt_task_prameters=defualt_task_prameters,
            async_query_interval=async_query_interval,
        )
                
class TaskStateMachinePool():
    
    def __init__(
        self,
        task_wrappers: Optional[List[TaskWrapper]] = None,
        thread_num: int = 0,
        process_num: int = 0,
        max_running_machines: int = 0,
        auto_close: bool = True, # close itself when all tasks are done
        progress: Union[TaskProgressManager,bool] = True,
    ) -> None:
        self.thread_num = thread_num
        self.process_num = process_num
        self.max_running_machines = max_running_machines
        self.auto_close = auto_close
        
        if progress is True:
            self.progress = TaskProgressManager()
        elif progress is False:
            self.progress = None
        else:
            self.progress = progress
            
        self.input_queue = queue.Queue()
        for task_wrapper in task_wrappers:
            self.input_queue.put(task_wrapper)
        self.output_queue = queue.Queue()
        self.init_workers()
        
        self.all_machines = {}
        self.running_machines = set()
        self.done_machines = set()
        self.waiting_machines = set()
        
    def init_workers(self):
        if self.thread_num > 0:
            self.thread_pool = ThreadPoolExecutor(max_workers=self.thread_num)
        elif self.thread_num < 0:
            self.thread_pool = ThreadPoolExecutor()
        else:
            self.thread_pool = None
        if self.process_num > 0:
            self.process_pool = ProcessPoolExecutor(max_workers=self.process_num)
        elif self.process_num < 0:
            self.process_pool = ProcessPoolExecutor()
        else:
            self.process_pool = None
            
    def shut_down_workers(self):
        if self.thread_pool is not None:
            self.thread_pool.shutdown(wait=True)
        if self.process_pool is not None:
            self.process_pool.shutdown(wait=True)
            
    @property
    def AllMachines(self) -> Dict[Hashable,TaskStateMachine]:
        return self.all_machines
    
    @property
    def InputQueue(self) -> queue.Queue:
        return self.input_queue
    
    @property
    def OutputQueue(self) -> queue.Queue:
        return self.output_queue
    
    @property
    def RunningMachines(self) -> Set[TaskStateMachine]:
        return self.running_machines
    
    @property
    def DoneMachines(self) -> Set[TaskStateMachine]:
        return self.done_machines
    
    @property
    def WaitingMachines(self) -> Set[TaskStateMachine]:
        return self.waiting_machines
    
    @property
    def Progress(self) -> Optional[TaskProgressManager]:
        return self.progress
        
    def open(self):
        self.init_workers()

    def close(self):
        self.shut_down_workers()
        
    def __enter__(self):
        self.open()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()
        
    def __len__(self):
        return len(self.all_machines)
    
    def get_machine_from_queue(self) -> Optional[TaskStateMachine]:
        pass
        
        
# ------------------------------ Test ------------------------------

import numpy as np

def dot(x,y):
    return np.dot(x,y)

class DotTask(AbstractTask):
    
    name = 'dot'
    worker = staticmethod(dot)
    input_names = (('x','y'),{})
    output_names = ('_d',)
    cache_vars = ('x','y')
    task_type = MAIN_TASK
    # worker_type = PROCESS_WORKER
    worker_type = COROUTINE_WORKER
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
    
    name = 'norm'
    # worker = staticmethod(norm)
    worker = staticmethod(norm_async)
    input_names = DICT
    output_names = DICT
    task_type = MAIN_TASK
    worker_type = COROUTINE_WORKER
    use_progress = True
    
def cosine(d,nx,ny):
    return np.dot(d,nx) / (nx*ny)
    
class CosineTask(AbstractTask):
    
    name = 'cosine'
    worker = staticmethod(cosine)
    input_names = (('_d','_x_norm','_y_norm'),{})
    output_names = ('cos',)
    task_type = MAIN_TASK
    worker_type = COROUTINE_WORKER
    use_progress = True

class CosineTaskConfig(TaskConfig):
    
    nodes = [
        [DotTask(), NormTask()],
        [CosineTask()],
    ]
    
def test_machine():
    cosine_task = CosineTaskConfig()
    with TaskProgressManager() as progress:
        with ThreadPoolExecutor() as thread_pool:
            with ProcessPoolExecutor() as process_pool:
                task_wrapper = cosine_task.get_task_wrapper(
                    machine_id=f'test',
                    initial_datas={'x':np.array([1,2,3]), 'y':np.array([4,5,6])},
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
    
async def test_machine_multi():
    cosine_task = CosineTaskConfig()
    with TaskProgressManager() as progress:
        with ThreadPoolExecutor() as thread_pool:
            with ProcessPoolExecutor() as process_pool:
                async with trio.open_nursery() as nursery:
                    for i in range(10000):
                        task_wrapper = cosine_task.get_task_wrapper(
                            machine_id=f'test_{i}',
                            initial_datas={'x':np.array([1,2,3]), 'y':np.array([4,5,6])},
                            progress=progress,
                            thread_pool=thread_pool,
                            process_pool=process_pool,
                            async_query_interval=0.1,
                        )
                        machine = TaskStateMachine(**task_wrapper)
                        nursery.start_soon(machine.start_asynchronously)
    print(machine.Datas)
    print('Done.')
    
if __name__ == '__main__':
    trio.run(test_machine_multi)
    # test_machine()