from __future__ import annotations
from rich.console import Console
from rich.progress import Progress, ProgressColumn, GetTimeCallable, TaskID, track
from typing import Union,Optional,Dict,Any,Hashable

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
        
class ProgressManager(Progress):
    
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
                
class FakeProgressManager():
    
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
    
    def __enter__(self) -> FakeProgressManager:
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