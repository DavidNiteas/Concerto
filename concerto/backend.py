import warnings
import asyncio
import nest_asyncio
nest_asyncio.apply()

HAS_TRIO_BACKEND = False
HAS_CURIO_BACKEND = False

try:
    import trio
    HAS_TRIO_BACKEND = True
except ImportError as e:
    warnings.warn("Trio backend is not installed.")
try:
    import curio
    HAS_CURIO_BACKEND = True
except ImportError as e:
    warnings.warn("Curio backend is not installed.")