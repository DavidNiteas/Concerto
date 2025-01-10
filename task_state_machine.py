from __future__ import annotations
import trio
import queue
import inspect
import uuid
import threading
import time
from dataclasses import dataclass
from rich.console import Console
from rich.progress import Progress, ProgressColumn, GetTimeCallable, TaskID, track
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

def get_kv_pairs(item:Union[list,dict],use_progress:bool=False,**kwargs):
    if isinstance(item,list):
        if use_progress:
            return enumerate(track(item,**kwargs))
        else:
            return enumerate(item)
    else:
        if use_progress:
            return track(item.items(),**kwargs)
        else:
            return item.items()
        
def dict2list(d:Dict[int,Any]) -> List[Any]:
    return [d[key] for key in sorted(d.keys())]

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
    
@dataclass    
class AbstractTask(BaseTask):
    """
    Abstract task class that defines a generic task structure for execution in a task state machine.

    Users should derive specific abstract tasks from this class by inheriting it and overriding the class attributes 
    to define the necessary behavior and configuration for different tasks. Although this class provides the possibility 
    to modify these parameters at the instance level, it is not recommended due to potential unexpected errors during 
    deserialization when the objects are read back in after being serialized.

    Attributes:
        name: The name of the task, which should be unique within the task pool.
        worker: The work function, which can be a callable or an asynchronous coroutine function.
        input_names: Definition of input parameters, indicating which inputs will be used from the machine's data.
        output_names: Definition of output parameters, indicating which outputs will be returned to the machine's data.
        task_type: Type of the task that indicates how the task will be executed (main task or side task).
        worker_type: Type of worker that indicates the concurrency method used for the task (coroutine, thread, or process).
        cache_vars: A list of variable names that should be deleted after the machine is closed.
        use_progress: A flag indicating whether to enable progress display.
        keep_worker_silent: A flag indicating whether to keep the worker silent, without printing any messages (not used).
    """

    name = None  # Task name
    worker = None  # Work function
    input_names = None  # Input parameter definition
    output_names = None  # Output parameter definition
    task_type = None  # Task type
    worker_type = None  # Worker type
    cache_vars = None  # Variables to delete after machine closure
    use_progress = None  # Flag for using progress bar
    keep_worker_silent = None  # Flag for silent worker


    def __init__(
        self,
        name: Hashable = None,  # Task name, which should be unique in the machine pool.
        worker: Callable = None,  # Work function that can be either a regular callable or an asynchronous coroutine function.
        input_names: Union[  # To specify which inputs will be used from machine.Datas
            Tuple[  # Define the structure of input arguments and keyword arguments
                Tuple[str,...],  # Input arguments; the machine will search for corresponding values in machine.Datas
                Dict[  # Input keyword arguments
                    str,  # The key in machine.Datas which the machine will use to find a corresponding value
                    str,  # The parameter name of the worker function
                ], 
            ],
            Literal[0],  # Use machine.Datas as a single input (may incur performance overhead with multiprocessing)
            Literal[1],  # No input provided to the task
            Callable[[Dict[str,Any]],Tuple[Tuple[Any,...],Dict[str,Any]]]  # Use a function to convert machine.Datas into the input dictionary
        ] = None,
        output_names: Union[  # To specify what outputs will be returned to machine.Datas
            Tuple[str,...],  # A tuple to define the output names; each element maps to an output name
            Literal[0],  # A single dictionary output, where the key is the output name
            Literal[1],  # No output from the task
            Callable[[Dict[str,Any],Any],None]  # Use a function to save the output into machine.Datas
        ] = None,
        task_type: Union[  # Definition of task type
            Literal[
                0,  # MAIN_TASK: if an error occurs in this task, the machine will stop and raise an exception
                1,  # SIDE_TASK: if an error occurs in this task, the machine will log a warning and save the error to the task
            ],
            None,  # If None, the machine will automatically determine the task type
        ] = None,
        worker_type: Union[  # Definition of worker type
            Literal[
                0,  # COROUTINE_WORKER: the worker will operate based on the trio framework
                1,  # THREAD_WORKER: the worker will operate using ThreadPoolExecutor
                2,  # PROCESS_WORKER: the worker will operate using ProcessPoolExecutor
            ],
            None,  # If None, the machine will automatically determine the worker type
        ] = None,
        cache_vars: Optional[Tuple[str,...]] = None,  # A tuple of variable names that will be deleted after the machine shuts down
        use_progress: Optional[bool] = None,  # If True, the worker will utilize a progress bar to display progress
        keep_worker_silent: Optional[bool] = None,  # If True, the worker will not output any messages to the console (not used)
        **kwargs  # Additional keyword arguments for further customization
    ):
        ''' 
        Initializes the AbstractTask with the provided parameters.

        Parameters:
            name (Hashable): A unique name for the task.
            
            worker (Callable): The function responsible for executing the task.
            
            input_names (Union[Tuple[Tuple[str,...], Dict[str, str]], Literal[0], Literal[1], Callable]):
                Defines the expected inputs for the task, which can be:
                - A tuple containing positional arguments and a dictionary of keyword arguments,
                - 0: DICT (use machine.Datas as the sole input for the task),
                - 1: NULL (no input is required for the task),
                - A function to convert machine.Datas into the required input dictionary,
                    - Callable[
                        [Dict[str, Any]], # machine.Datas will input into the function
                        Tuple[Tuple[Any,...], Dict[str, Any]]  # output the required args and kwargs
                      ]
                
            output_names (Union[Tuple[str,...], Literal[0], Literal[1], Callable]):
                Defines the expected outputs for the task, which can be:
                - A tuple of output names,
                - 0: DICT (provide the output as a single dictionary),
                - 1: NULL (the task does not produce any output),
                - A function that saves the output into machine.Datas.
                    - Callable[
                        [
                            Dict[str,Any],  # machine.Datas
                            Any, # the result of the worker function
                        ],
                        None, # return nothing
                      ]
            
            task_type (Union[Litteral[0], Literal[1], None]): 
                Indicates the task type which determines its error handling behavior:
                - 0: MAIN_TASK (if an error occurs, the machine will stop and raise an exception),
                - 1: SIDE_TASK (if an error occurs, the machine will warn and log the error).
            
            worker_type (Union[Litteral[0], Literal[1], Literal[2], None]): 
                Specifies the execution model for the worker:
                - 0: COROUTINE_WORKER (operates based on the trio framework),
                - 1: THREAD_WORKER (operates using ThreadPoolExecutor),
                - 2: PROCESS_WORKER (operates using ProcessPoolExecutor).
            
            cache_vars (Optional[Tuple[str,...]]): 
                A list of variable names that should be discarded upon machine shutdown.
            
            use_progress (Optional[bool]): 
                Indicates whether to display a progress bar during execution.
            
            keep_worker_silent (Optional[bool]): 
                If True, suppresses any console output from the worker.
            
            **kwargs: Additional parameters for further customization.
        '''
        
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
    """
    Represents an instance of an abstract task that includes runtime information about the task's state and error status.

    This class encapsulates a specific execution of an AbstractTask and adds functionality to track the current state 
    of the task as well as any errors that may occur during its execution.

    Attributes:
        abstract_task (AbstractTask): The corresponding abstract task that this instance represents.
        state (Literal[-1, 0, 1]): The current state of the task, where:
            -1 indicates that the task has not begun,
            0 indicates that the task is in progress, and
            1 indicates that the task has completed successfully.
        error (Optional[Exception]): The error information if an error occurs during the execution of the task; otherwise, it is None.
    """
    
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
    """
    Represents a machine that has not yet begun execution.

    This class inherits from BaseTask and defines the state of a task that is still in the initial state.
    It indicates that the task pipeline is ready to start but has not yet commenced its execution.

    Properties:
        State (Literal[1]): Returns 1, this task just indicates that the machine has not started yet.
        is_all_done (bool): Returns True.
    """
    
    @property
    def State(self) -> Literal[1]:
        return 1
    
    @property
    def is_all_done(self) -> bool:
        return True
    
class EndTask(BaseTask):
    """
    Represents a machine that has completed execution.

    This class inherits from BaseTask and indicates that the task has finished its execution.
    It signifies that all operations have been carried out and the task is now in the completed state.

    Properties:
        State (Literal[1]): Returns 1, this task just indicates that the machine has completed its execution.
        is_all_done (bool): Returns True.
    """
    
    @property
    def State(self) -> Literal[1]:
        return 1
    
    @property
    def is_all_done(self) -> bool:
        return True

class TaskGroup(BaseTask):
    """
    Represents a group of tasks that can be managed collectively.

    This class inherits from BaseTask and provides functionality to manage multiple tasks as a single unit.
    It allows for tracking the state of all tasks, managing cache variables, and checking if all tasks are completed or running.

    Attributes:
        tasks (Dict[Hashable, Task]): A dictionary of tasks, where the key is the task name and the value is the Task object.
        cache_vars (FrozenSet[str]): A set of cache variables that should be deleted after the machine shuts down.
    """
    
    def __init__(
        self,
        tasks: Union[Sequence[Task],Task],
    ):
        """
        Initializes the TaskGroup with the provided tasks.

        Parameters:
            tasks (Union[Sequence[Task], Task]): A single task or a sequence of tasks to be included in the group.
                If a single Task is provided, it will be converted into a list.

        The constructor initializes the tasks dictionary and collects cache variables from all tasks.
        """
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
    """
    Represents a task map consisting of multiple linear execution nodes, where each node is a TaskGroup containing several tasks.

    This class essentially functions as a generator for managing the execution flow of tasks. During execution, 
    the machine treats this class as a generator to yield nodes sequentially. Each node, which is an instance of TaskGroup,
    contains multiple tasks that the machine executes in parallel. 

    The machine will request the next node only after all tasks in the current node have completed. 
    Once all tasks in the entire task map are finished, the class will output an EndTask instance to signal the machine to shut down.

    Attributes:
        nodes (List[TaskGroup]): A list of TaskGroup instances representing the linear execution nodes.
        pointer (int): A pointer to track the current position in the nodes list.
        cache_vars (FrozenSet[str]): A set of cache variables collected from all TaskGroups in the map.
    """
    
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
        description: Optional[str] = None,
    ) -> None:
        if task_name in self.Name2ID:
            if completed is None:
                if self._tasks[self.Name2ID[task_name]].total is None:
                    self.update(task_name, total=advance, description=description)
                else:
                    self.update(task_name, total=self._tasks[self.Name2ID[task_name]].total+advance, description=description)
            else:
                self.update(task_name, total=completed, description=description)
                
class FakeTaskProgressManager():
    
    def __init__(self):
        pass
    
    def add_task(self, *args, **kwargs):
        pass
    
    def update(self, *args, **kwargs):
        pass
    
    def update_total(self, *args, **kwargs):
        pass
    
    def start(self):
        pass
    
    def stop(self):
        pass
    
    def __enter__(self) -> FakeTaskProgressManager:
        self.start()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()
        pass
    
class FakePool():
    
    def __enter__(self):
        return None
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

class TaskStateMachine():
    """
    Represents a state machine for managing the execution of tasks within a task framework.

    Attributes:
        defualt_task_prameters (Dict[str, Any]): Default task configuration parameters.
        async_query_interval (float): Interval for querying the state of asynchronous tasks.
        
    Methods:
        from_task_wrapper(wrapper: TaskWrapper) -> TaskStateMachine:
            Creates an instance from a TaskWrapper.

        __init__(machine_id: Hashable, task_map: TaskMap, initial_datas: Dict[str, Any], ...):
            Initializes the state machine with the provided configuration.

        start_serially() -> None:
            Executes tasks in the task map sequentially.

        start_asynchronously() -> None:
            Starts the execution of tasks asynchronously.

        start_multiple_machines(...):
            Starts multiple TaskStateMachine instances based on the provided configurations.
    """
    
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
        initial_datas: Dict[str, Any],
        thread_pool: Optional[ThreadPoolExecutor] = None,
        process_pool: Optional[ProcessPoolExecutor] = None,
        progress: Optional[TaskProgressManager] = None,
        defualt_task_prameters: Optional[Dict[str, Any]] = None,
        async_query_interval: Optional[float] = None,
    ):
        """
        Initializes the TaskStateMachine with the given parameters.

        Parameters:
            machine_id (Hashable): A unique identifier for the state machine instance.
            task_map (TaskMap): A map of tasks that the state machine will manage.
            initial_datas (Dict[str, Any]): A dictionary of initial input data for the tasks.
            thread_pool (Optional[ThreadPoolExecutor], optional): An optional thread pool for executing tasks concurrently. Defaults to None.
            process_pool (Optional[ProcessPoolExecutor], optional): An optional process pool for executing tasks concurrently. Defaults to None.
            progress (Optional[TaskProgressManager], optional): An optional progress manager for tracking task execution progress. Defaults to None.
            defualt_task_prameters (Optional[Dict[str, Any]], optional): A dictionary of default parameters to configure tasks. Defaults to None.
            async_query_interval (Optional[float], optional): The interval (in seconds) for querying asynchronous task states. Defaults to None.

        This constructor sets up the state machine and opens it for task execution with the specified resources and configurations.
        """

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
        """
        Executes all tasks in the task map sequentially in a synchronous manner.

        This method sets the state of the task state machine to RUNNING and processes each task group one by one.
        It will run each task in the order they are defined, ensuring that tasks are fully completed before moving 
        on to the next one. The execution continues until either an EndTask is encountered, signaling the completion 
        of all tasks, or the state of the machine is changed.

        Note:
            This method is designed for synchronous execution and is useful for debugging tasks, allowing for a 
            straightforward tracing of task execution flow. It should be used in debug mode to easily identify 
            issues with task execution order or dependencies.

        After all tasks have been processed, the method calls the `close()` function to clean up resources.
        """

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
    
    async def init_worker_input_coroutine(
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
    
    async def start_worker_coroutine(
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
        
    async def update_machine_data_coroutine(
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

    async def run_task_coroutine(
        self,
        task: Task,
    ) -> None:
        task.State = RUNNING
        try:
            self.init_task_parameters(task)
            input_args, input_kwargs = await self.init_worker_input_coroutine(task)
            worker_output = await self.start_worker_coroutine(task, input_args, input_kwargs)
            await self.update_machine_data_coroutine(worker_output, task)
        except Exception as e:
            if task.TaskType != MAIN_TASK:
                task.Error = e
                warnings.warn(f'Task:{task.Name} in Machine:{self.ID} failed with error:{e}')
            else:
                raise e
        self.update_task_progress(task)
        task.State = END
 
    async def start_asynchronously_coroutine(self):
        """
        Executes tasks in the task map asynchronously using Trio.

        This coroutine sets the state of the task state machine to RUNNING and processes each task group using 
        asynchronous execution. It will retrieve task groups and if the group is not an EndTask or NotBeginTask, 
        it will check if all tasks in the group are running. If not, it starts each task that has not begun 
        using a nursery, allowing for concurrent execution of tasks within the same group.

        The method periodically awaits a sleep interval defined by `async_query_interval` to yield control back 
        to the event loop, allowing other asynchronous tasks to run while waiting.

        After all tasks have been processed, the method calls `close()` to clean up resources.
        """
        
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
                                nursery.start_soon(self.run_task_coroutine, task)
                                task.State = RUNNING
            await trio.sleep(self.async_query_interval)
        self.close()
        
    def start_asynchronously(self):
        """
        Initiates the asynchronous execution of tasks by running the asynchronous coroutine.

        This method serves as a wrapper for the `start_asynchronously_coroutine` coroutine and is designed to 
        be called in a synchronous context. It uses Trio's `run` method to execute the coroutine, which will 
        start processing tasks according to the logic defined in `start_asynchronously_coroutine`.
        """
        trio.run(self.start_asynchronously_coroutine)
        
    @classmethod
    async def start_multiple_machines_coroutine(
        cls,
        task_configs: Union[TaskConfig,List[TaskConfig],Dict[Hashable,TaskConfig]],
        initial_data_list: Union[List[Dict[str,Any]],Dict[Hashable,Dict[str,Any]]],
        thread: Union[int,ThreadPoolExecutor] = 0,
        process: Union[int,ProcessPoolExecutor] = 0,
        progress: Union[TaskProgressManager,bool] = True,
    ) -> Union[List[TaskStateMachine],Dict[Hashable,TaskStateMachine]]:
        """
        Starts multiple instances of TaskStateMachine asynchronously using the provided configurations.

        This class method initializes the necessary resources for running multiple machines in parallel. 
        It sets up progress tracking, thread pooling, and process pooling based on the inputs received. 
        Within an asynchronous nursery, it creates and starts each TaskStateMachine instance, running them 
        concurrently in the event loop.

        Parameters:
            task_configs (Union[TaskConfig, List[TaskConfig], Dict[Hashable, TaskConfig]]): 
                Configuration for the tasks to be executed by the state machines.
            initial_data_list (Union[List[Dict[str, Any]], Dict[Hashable, Dict[str, Any]]): 
                Initial data for each machine, either as a list or a dictionary keyed by machine ID.
            thread (Union[int, ThreadPoolExecutor], optional): Number of threads or a thread pool for concurrent execution. Defaults to 0.
            process (Union[int, ProcessPoolExecutor], optional): Number of processes or a process pool for concurrent execution. Defaults to 0.
            progress (Union[TaskProgressManager, bool], optional): A progress manager for tracking execution or a boolean to enable/disable progress. Defaults to True.

        Returns:
            Union[List[TaskStateMachine], Dict[Hashable, TaskStateMachine]]: 
                A list or dictionary of TaskStateMachine instances corresponding to the provided configurations.

        Note:
            This method is particularly useful for users who want to quickly parallelize a series of complex tasks 
            within an existing asynchronous context.
        """
        if isinstance(progress,bool):
            if progress:
                progress = TaskProgressManager()
            else:
                progress = FakeTaskProgressManager()
        if isinstance(thread,int):
            if thread > 0:
                thread = ThreadPoolExecutor(max_workers=thread)
            else:
                thread = FakePool()
        else:
            thread = thread
        if isinstance(process,int):
            if process > 0:
                process = ProcessPoolExecutor(max_workers=process)
            else:
                process = FakePool()
        else:
            process = process
        machines: Dict[Hashable,TaskStateMachine] = {}
        with progress, thread, process:
            async with trio.open_nursery() as nursery:
                for machine_id,initial_data in get_kv_pairs(initial_data_list):
                    task_config = task_configs[machine_id] if not isinstance(task_configs,TaskConfig) else task_configs
                    task_wrapper = task_config.get_task_wrapper(
                        initial_data,
                        machine_id=machine_id,
                        progress=progress,
                        thread_pool=thread,
                        process_pool=process,
                    )
                    machines[machine_id] = TaskStateMachine(**task_wrapper)
                    nursery.start_soon(machines[machine_id].start_asynchronously_coroutine)
        if isinstance(initial_data_list,list):
            machines = dict2list(machines)
        return machines
    
    @classmethod
    def start_multiple_machines(
        cls,
        task_configs: Union[TaskConfig,List[TaskConfig],Dict[Hashable,TaskConfig]],
        initial_data_list: Union[List[Dict[str,Any]],Dict[Hashable,Dict[str,Any]]],
        thread: Union[int,ThreadPoolExecutor] = 0,
        process: Union[int,ProcessPoolExecutor] = 0,
        progress: Union[TaskProgressManager,bool] = True,
    ) -> Union[List[TaskStateMachine],Dict[Hashable,TaskStateMachine]]:
        """
        Starts multiple instances of TaskStateMachine synchronously.

        This method serves as a blocking call that runs the asynchronous coroutine
        `start_multiple_machines_coroutine` within a Trio event loop. It allows multiple 
        TaskStateMachine instances to be created and started in a parallel manner as defined 
        by the provided configurations.

        Parameters:
            task_configs (Union[TaskConfig, List[TaskConfig], Dict[Hashable, TaskConfig]]): 
                Configuration for the tasks to be executed by the state machines.
            initial_data_list (Union[List[Dict[str, Any]], Dict[Hashable, Dict[str, Any]]): 
                Initial data for each machine, either as a list or a dictionary keyed by machine ID.
            thread (Union[int, ThreadPoolExecutor], optional): Number of threads or a thread pool for concurrent execution. Defaults to 0.
            process (Union[int, ProcessPoolExecutor], optional): Number of processes or a process pool for concurrent execution. Defaults to 0.
            progress (Union[TaskProgressManager, bool], optional): A progress manager for tracking execution or a boolean to enable/disable progress. Defaults to True.

        Returns:
            Union[List[TaskStateMachine], Dict[Hashable, TaskStateMachine]]: 
                A list or dictionary of TaskStateMachine instances corresponding to the provided configurations.

        Note:
            This method is ideal for users who wish to execute tasks quickly in a synchronous context 
            without embedding into an existing asynchronous flow.
        """
        
        return trio.run(
            cls.start_multiple_machines_coroutine, 
            task_configs, 
            initial_data_list,
            thread,process,progress,
        )
        
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
    """
    Represents the configuration for a task process, serving as an abstraction layer for defining a task graph.

    This class allows users to start with an abstract representation of a task graph, defined by a list of lists of 
    AbstractTask instances. It facilitates the creation of executable TaskWrapper instances by providing the necessary 
    data and execution parameters. Users can define default parameters, initial data, and other configurations for 
    executing tasks within a state machine.

    Users are encouraged to derive new task configurations by inheriting this class and overriding the class attributes 
    to customize the task nodes and parameters as needed. This approach allows for flexibility in adapting the 
    configuration for various use cases.

    Attributes:
        nodes (List[List[AbstractTask]]): A list of task nodes, where each node is a list of AbstractTask instances.
        defualt_task_prameters (Dict[str, Any]): Default parameters for task execution.
        async_query_interval (float): The interval (in seconds) for querying the state of asynchronous tasks.
        defualt_initial_datas (Dict[str, Any]): Default initial data to be used for the tasks.
    """

    
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
        initial_datas: Dict[str,Any],
        machine_id: Optional[Hashable] = None,
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
    """
    **TaskStateMachinePool**
    
    Represents a unified scheduler for managing multiple TaskStateMachine instances.

    The TaskStateMachinePool is designed to coordinate the execution of multiple task machines, 
    enabling concurrent or sequential processing of tasks based on the provided configurations. 
    This class manages the lifecycle of task machines, including initialization, execution, 
    and monitoring of their states.

    **Key Attributes:**
    - **name (Hashable)**: A unique identifier for the task state machine pool.
    - **thread_num (int)**: Number of threads available for executing tasks.
    - **process_num (int)**: Number of processes available for executing tasks.
    - **max_running_machines (int)**: The maximum number of task machines that can run simultaneously.
    - **auto_close (bool)**: Determines whether the pool should close itself when all tasks are completed.
    - **monitor_interval (float)**: The interval (in seconds) at which the pool checks the status of running machines.
    - **progress (Union[TaskProgressManager, bool])**: Manages the progress of task execution, allowing for 
        visual feedback on processing status.
    - **input_queue (queue.Queue)**: Queue for incoming task wrappers to be processed.
    - **output_queue (queue.Queue)**: Queue for outputting completed task machines.
    - **all_machines (Dict[Hashable, TaskStateMachine])**: Dictionary storing all task machines by their IDs.
    - **running_machines (Set[TaskStateMachine])**: Set of currently running task machines.
    - **done_machines (Set[TaskStateMachine])**: Set of completed task machines.
    - **waiting_machines (Set[TaskStateMachine])**: Set of task machines waiting to be executed.

    **Methods:**
    - **start_serially() -> None**:
        Starts executing all task machines in a synchronous manner.

    - **start_asynchronously() -> None**:
        Starts executing all task machines in an asynchronous manner, allowing for concurrent processing.

    - **start_serially_by_thread() -> Optional[Tuple[threading.Thread, threading.Thread]]**:
        Starts task machines using threads for loading and monitoring while allowing for concurrent execution.

    - **start_asynchronously_by_thread() -> Optional[Tuple[threading.Thread, threading.Thread]]**:
        Starts task machines asynchronously using threads.
    """
    
    def __init__(
        self,
        task_wrappers: Optional[List[TaskWrapper]] = None,
        name: Hashable = None,
        thread_num: int = 0,
        process_num: int = 0,
        max_running_machines: int = 0,
        auto_close: bool = True, # close itself when all tasks are done
        progress: Union[TaskProgressManager,bool] = True,
        monitor_interval: float = 0.1,
    ) -> None:
        if name is None:
            self.name = uuid.uuid4()
        else:
            self.name = name
        self.thread_num = thread_num
        self.process_num = process_num
        self.max_running_machines = max_running_machines
        self.auto_close = auto_close
        self.monitor_interval = monitor_interval
        
        if progress is True:
            self.progress = TaskProgressManager()
        elif progress is False:
            self.progress = FakeTaskProgressManager()
        else:
            self.progress = progress
            
        self.input_queue = queue.Queue()
        self.output_queue = queue.Queue()
        self.init_workers()
        
        self.all_machines = {}
        self.running_machines = set()
        self.done_machines = set()
        self.waiting_machines = set()
        
        for i,task_wrapper in enumerate(task_wrappers):
            if isinstance(task_wrapper, TaskWrapper):
                if task_wrapper.MachineID is None:
                    task_wrapper.MachineID = i
                if task_wrapper.ThreadPool is None:
                    task_wrapper.ThreadPool = self.thread_pool
                if task_wrapper.ProcessPool is None:
                    task_wrapper.ProcessPool = self.process_pool
                if task_wrapper.Progress is None:
                    task_wrapper.Progress = self.progress
                self.AllMachines[task_wrapper.MachineID] = TaskStateMachine(**task_wrapper)
                self.WaitingMachines.add(task_wrapper.MachineID)
                self.Progress.update_total(f'Pool {self.name} running')
        
        #runtime variables
        self.running = False
        self.loader_running = False
        self.monitor_running = False
        
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
    def Progress(self) -> Union[TaskProgressManager,FakeTaskProgressManager]:
        return self.progress
    
    @property
    def Running(self) -> bool:
        return self.running
    
    def join(self):
        while self.Running:
            time.sleep(self.monitor_interval)
        
    def open(self):
        self.init_workers()
        self.Progress.start()

    def close(self):
        self.join()
        self.shut_down_workers()
        self.Progress.update(
            f'Pool {self.name} running',
            completed=len(self.all_machines),
            total=len(self.all_machines),
            description=f'Pool {self.name}: {len(self.done_machines)} machines done.',
        )
        self.Progress.stop()
        
    def __enter__(self):
        self.open()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()
        
    def __len__(self):
        return len(self.all_machines)
    
    # ↓↓↓↓↓ Synchronous Methods ↓↓↓↓↓
    
    def get_machine_from_queue(self) -> Union[TaskStateMachine,EndTask]:
        if self.auto_close and self.input_queue.empty():
            return EndTask()
        wrapper = self.input_queue.get()
        if isinstance(wrapper, TaskWrapper):
            if wrapper.MachineID is None:
                wrapper.MachineID = uuid.uuid4()
            if wrapper.ThreadPool is None:
                wrapper.ThreadPool = self.thread_pool
            if wrapper.ProcessPool is None:
                wrapper.ProcessPool = self.process_pool
            if wrapper.Progress is None:
                wrapper.Progress = self.progress
            return TaskStateMachine(**wrapper)
        elif isinstance(wrapper, EndTask):
            return wrapper
        else:
            raise ValueError(f'The input of TaskStateMachinePool should be TaskWrapper or EndTask, but found {type(wrapper)}')
        
    def run_machine_loader(self) -> None:
        self.loader_running = True
        while self.loader_running:
            machine = self.get_machine_from_queue()
            if isinstance(machine, EndTask):
                self.loader_running = False
            else:
                if machine.ID in self.all_machines:
                    warnings.warn(f'Machine:{machine.ID} is already in this pool, we will use the old one')
                else:
                    self.all_machines[machine.ID] = machine
                    self.WaitingMachines.add(machine.ID)
                    self.Progress.update_total(f'Pool {self.name} running')
    
    def run_machine_monitor(self) -> None:
        self.monitor_running = True
        while self.monitor_running:
            tag_mids = self.RunningMachines | self.WaitingMachines
            if len(tag_mids) == 0 and not self.loader_running:
                self.monitor_running = False
            for mid in tag_mids:
                machine = self.AllMachines[mid]
                if machine.State == NOT_BEGIN:
                    if self.max_running_machines <= 0 or len(self.running_machines) < self.max_running_machines:
                        self.RunningMachines.add(mid)
                        self.WaitingMachines.remove(mid)
                        machine.start_serially()
                elif machine.State == END:
                    self.DoneMachines.add(mid)
                    self.RunningMachines.remove(mid)
                    self.output_queue.put(machine)
                    self.Progress.update(f'Pool {self.name} running',advance=1)
            time.sleep(self.monitor_interval)
        self.running = False
        
    def start_serially(self) -> None:
        """
        Starts executing all task machines in a synchronous and linear manner.

        This method is used to begin the processing of tasks in the TaskStateMachinePool one after the other, 
        ensuring that each task is fully completed before the next one begins. It sets the `running` state to 
        True, adds a progress task for monitoring, and initiates both the machine loader and monitor processes.

        This method is particularly useful during debugging, as it allows for straightforward tracking of task 
        execution and diagnosing potential issues in the sequential flow of tasks. If the pool is already running, 
        it raises a warning and does not start the process again.
        """

        if self.running:
            warnings.warn('TaskStateMachinePool is already running, we will not start it again')
        else:
            self.running = True
            self.progress.add_task(f'Pool {self.name} running')
            self.run_machine_loader()
            self.run_machine_monitor()
            self.auto_close = False
    
    def start_serially_by_thread(
        self,
    ) -> Optional[Tuple[
        threading.Thread, # loader thread
        threading.Thread, # monitor thread
    ]]:
        """
        Starts the task loading and monitoring processes using two separate threads.

        This method initiates the task loading process and the monitoring process as two distinct threads,
        allowing them to run concurrently. However, within each thread, the execution of tasks remains linear, 
        processing them one by one in the order they are queued.

        The `loader` thread is responsible for loading tasks from the input queue, while the `monitor` thread 
        oversees the state of running machines. This setup allows for non-blocking execution and can improve 
        responsiveness, as both functions can operate independently.

        If the TaskStateMachinePool is already running, a warning is raised and the method will not start the 
        processes again. 

        Returns:
            Optional[Tuple[threading.Thread, threading.Thread]]:
                A tuple containing the two thread objects (loader and monitor). If the pool is already running,
                this method will return None, indicating that the processes were not started.
        """

        if self.running:
            warnings.warn('TaskStateMachinePool is already running, we will not start it again')
        else:
            self.running = True
            self.progress.add_task(f'Pool {self.name} running')
            loader = threading.Thread(target=self.run_machine_loader, daemon=True)
            monitor = threading.Thread(target=self.run_machine_monitor, daemon=True)
            loader.start()
            monitor.start()
            return loader, monitor
                    
    def get_done_machines(self) -> List[TaskStateMachine]:
        """
        Retrieves a list of completed task machines from the output queue.

        This method collects all task machines that have finished their execution and are available
        in the output queue. It iterates through the queue, removing each completed machine and appending
        it to a list, which is then returned.

        Returns:
            List[TaskStateMachine]: A list of TaskStateMachine instances that have completed their tasks.
        """

        done_machines = []
        while not self.output_queue.empty():
            done_machines.append(self.output_queue.get())
        return done_machines
    
    # ↓↓↓↓↓ Asynchronous Methods ↓↓↓↓↓
    
    async def run_machine_loader_coroutine(self) -> None:
        self.run_machine_loader()
                    
    async def run_machine_monitor_coroutine(self) -> None:
        self.monitor_running = True
        async with trio.open_nursery() as nursery:
            while self.monitor_running:
                tag_mids = self.RunningMachines | self.WaitingMachines
                if len(tag_mids) == 0 and not self.loader_running:
                    self.monitor_running = False
                for mid in tag_mids:
                    machine = self.AllMachines[mid]
                    if machine.State == NOT_BEGIN:
                        if self.max_running_machines <= 0 or len(self.running_machines) < self.max_running_machines:
                            self.RunningMachines.add(mid)
                            self.WaitingMachines.remove(mid)
                            nursery.start_soon(machine.start_asynchronously_coroutine)
                    elif machine.State == END:
                        self.DoneMachines.add(mid)
                        self.RunningMachines.remove(mid)
                        self.output_queue.put(machine)
                        self.Progress.update(f'Pool {self.name} running',advance=1)
                await trio.sleep(self.monitor_interval)
        self.running = False
        
    def run_machine_monitor_async(self) -> None:
        trio.run(self.run_machine_monitor_coroutine)
        
    async def start_asynchronously_coroutine(self) -> None:
        """
        Initiates the TaskStateMachinePool in an asynchronous context.

        This method is one of the primary ways to start the pool, allowing it to be embedded within an existing 
        asynchronous process. When called, it sets the `running` state to True and adds a task for tracking 
        progress. It then uses Trio's nursery to concurrently start both the task loading process and the 
        monitoring process.

        This approach allows the pool to operate without blocking the main event loop, making it suitable for 
        applications that require non-blocking task management alongside other asynchronous operations. If 
        the pool is already running, a warning is issued, and the method will not restart the processes.
        """

        if self.running:
            warnings.warn('TaskStateMachinePool is already running, we will not start it again')
        else:
            self.running = True
            self.progress.add_task(f'Pool {self.name} running')
            async with trio.open_nursery() as nursery:
                self.loader_running = True
                self.monitor_running = True
                nursery.start_soon(self.run_machine_loader_coroutine)
                nursery.start_soon(self.run_machine_monitor_coroutine)
                
    def start_asynchronously(self) -> None:
        """
        Starts the TaskStateMachinePool using an asynchronous approach within a synchronous environment.

        This method serves as a major starting point for the pool, allowing it to be initiated in an 
        asynchronous manner while using a synchronous context. It utilizes Trio's `run` function to execute 
        the `start_asynchronously_coroutine`, which launches both the task loading and monitoring processes.

        By doing so, the loader and monitor operate asynchronously, sharing a single event loop. This enables 
        efficient task management and monitoring without blocking the main thread. If the pool is already running, 
        it will raise a warning and not initiate the processes again.
        """

        trio.run(self.start_asynchronously_coroutine)
                    
    def start_asynchronously_by_thread(self) -> Optional[Tuple[threading.Thread, threading.Thread]]:
        """
        Initiates the TaskStateMachinePool using threads to run the task loading and monitoring processes.

        This method is one of the primary ways to start the pool, allowing the loader and monitor to operate
        concurrently in separate threads. It creates and starts one thread dedicated to loading tasks 
        asynchronously and another thread for monitoring the state of those tasks.

        If the TaskStateMachinePool is already running, a warning is issued and the method will not restart 
        the processes.

        Returns:
            Optional[Tuple[threading.Thread, threading.Thread]]:
                A tuple containing the two thread objects for the loader and monitor. If the pool is 
                already running, this method will return None, indicating that the processes were not started.
        """

        if self.running:
            warnings.warn('TaskStateMachinePool is already running, we will not start it again')
        else:
            self.running = True
            self.progress.add_task(f'Pool {self.name} running')
            loader = threading.Thread(target=self.run_machine_loader, daemon=True)
            monitor = threading.Thread(target=self.run_machine_monitor_async, daemon=True)
            loader.start()
            monitor.start()
            return loader, monitor
        
# ------------------------------ Test ------------------------------

import numpy as np

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
    with TaskProgressManager() as progress:
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