import time
from abc import ABC, abstractmethod
from collections import OrderedDict
from typing import Dict, List, Union

from pydantic import BaseModel

import sparkplub_b_packets.core.sparkplug_b_pb2 as sparkplug_b_pb2
import sparkplub_b_packets.core.sparkplug_b as sparkplug
from sparkplub_b_packets.core.sparkplug_b import addMetric
from sparkplub_b_packets.helpers.logging_helpers import get_logger
log = get_logger(__name__)


class MetricProperty(BaseModel):
    name: str
    data_type: int
    value: Union[int, float, bool]


class Metric(BaseModel):
    alias: str
    alias_map: int
    data_type: int
    value: Union[int, float, bool]
    properties: List[MetricProperty] = None


class SparkplugBPacket(ABC):

    def __init__(self, group: str, node: str):
        self._group = group
        self._node = node

    @staticmethod
    def marshal_payload(payload, metrics: Dict[str, Metric] = None) -> str:
        for alias in list(metrics):
            metric = addMetric(
                container=payload,
                name=alias,
                alias=metrics[alias].alias_map,
                type=metrics[alias].data_type,
                value=metrics[alias].value
            )
            if metrics[alias].properties:
                for prop in metrics[alias].properties:
                    metric.properties.keys.extend([prop.name])
                    propertyValue = metric.properties.values.add()
                    propertyValue.type = prop.data_type
                    if isinstance(prop.value, bool):
                        propertyValue.bool_value = prop.value
                    elif isinstance(prop.value, int):
                        propertyValue.int_value = prop.value
                    elif isinstance(prop.value, float):
                        propertyValue.float_value = prop.value
                    else:
                        propertyValue.string_value = str(prop.value)

        return payload.SerializeToString()

    @property
    def group(self):
        return self._group.lower().replace(".", "X")

    @property
    def node(self):
        return self._node

    @property
    @abstractmethod
    def topic(self) -> str:
        pass

    @property
    def metrics(self) -> Dict[str, Metric]:
        return OrderedDict()

    @abstractmethod
    def payload(self, metrics: Dict[str, Metric] = None) -> str:
        pass

    def __str__(self):
        return f"topic: {self.topic}\npayload: {self.payload()}"


class NBirthPacket(SparkplugBPacket):

    def __init__(self, group: str, node: str, metrics: Dict[str, Metric]):
        super().__init__(group, node)
        self._metrics = metrics

    @property
    def topic(self) -> str:
        return f"spBv1.0/{self.group}/NBIRTH/{self._node}"

    @property
    def metrics(self):
        return self._metrics

    def payload(self, metrics: Dict[str, Metric] = None) -> str:
        log.debug(f"Birth pre bdSeq: {sparkplug.bdSeq}")
        payload = sparkplug.getNodeBirthPayload()
        log.debug(f"Birth post bdSeq: {sparkplug.bdSeq}")
        return self.marshal_payload(payload, metrics if metrics else self.metrics)


class NDataPacket(SparkplugBPacket):

    def __init__(self, group: str, node: str, metrics: Dict[str, Metric] = None):
        super().__init__(group, node)
        self._metrics = metrics if metrics else {}

    @property
    def topic(self) -> str:
        return f"spBv1.0/{self.group}/NDATA/{self._node}"

    @property
    def metrics(self):
        return self._metrics

    def payload(self, metrics: Dict[str, Metric] = None) -> str:
        payload = sparkplug.getDeviceBirthPayload()
        return self.marshal_payload(payload, metrics if metrics else self.metrics)


class NCmdPacket(SparkplugBPacket):

    def __init__(self, group: str, node: str, metrics: Dict[str, Metric] = None):
        super().__init__(group, node)
        self._metrics = metrics if metrics else {}

    @property
    def topic(self) -> str:
        return f"spBv1.0/{self.group}/NCMD/{self._node}"

    @property
    def metrics(self):
        return self._metrics

    def payload(self, metrics: Dict[str, Metric] = None) -> str:
        payload = sparkplug_b_pb2.Payload()
        payload.timestamp = int(round(time.time() * 1000))
        return self.marshal_payload(payload, metrics if metrics else self.metrics)


class NDeathPacket(NBirthPacket):

    def __init__(self, group: str, node: str):
        super().__init__(group=group, node=node, metrics={})

    @property
    def topic(self) -> str:
        return f"spBv1.0/{self.group}/NDEATH/{self._node}"

    def payload(self, metrics: Dict[str, Metric] = None) -> str:
        log.debug(f"Death pre bdSeq: {sparkplug.bdSeq}")
        deathPayload = sparkplug.getNodeDeathPayload()
        log.debug(f"Death pre bdSeq: {sparkplug.bdSeq}")
        return deathPayload.SerializeToString()


class DBirthPacket(SparkplugBPacket):

    def __init__(self, group: str, node: str, device_id: str, metrics: Dict[str, Metric]):
        super().__init__(group, node)
        self._device_id = device_id
        self._metrics = metrics

    @property
    def topic(self) -> str:
        return f"spBv1.0/{self.group}/DBIRTH/{self._node}/{self._device_id}"

    @property
    def metrics(self):
        return self._metrics

    def payload(self, metrics: Dict[str, Metric] = None) -> str:
        payload = sparkplug.getDeviceBirthPayload()
        return self.marshal_payload(payload, metrics if metrics else self.metrics)


class DDataPacket(DBirthPacket):

    @property
    def topic(self) -> str:
        return f"spBv1.0/{self.group}/DDATA/{self._node}/{self._device_id}"

    def payload(self, metrics: Dict[str, Metric] = None) -> str:
        payload = sparkplug.getDeviceBirthPayload()
        return self.marshal_payload(payload, metrics if metrics else self.metrics)


class DCmdPacket(SparkplugBPacket):

    def __init__(self, group: str, node: str, metrics: Dict[str, Metric] = None):
        super().__init__(group, node)
        self._metrics = metrics if metrics else {}

    @property
    def topic(self) -> str:
        return f"spBv1.0/{self.group}/DCMD/{self._node}"

    @property
    def metrics(self):
        return self._metrics

    def payload(self, metrics: Dict[str, Metric] = None) -> str:
        payload = sparkplug_b_pb2.Payload()
        payload.timestamp = int(round(time.time() * 1000))
        return self.marshal_payload(payload, metrics if metrics else self.metrics)


class DDeathPacket(DBirthPacket):

    def __init__(self, group: str, node: str, device_id: str):
        super().__init__(group=group, node=node, device_id=device_id, metrics={})

    @property
    def topic(self) -> str:
        return f"spBv1.0/{self.group}/DDEATH/{self._node}/{self._device_id}"

    def payload(self, metrics: Dict[str, Metric] = None) -> str:
        payload = sparkplug.getDeviceBirthPayload()
        return payload.SerializeToString()


