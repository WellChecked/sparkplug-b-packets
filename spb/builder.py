from abc import ABC, abstractmethod

from spb.packets import DBirthPacket, DDataPacket, DDeathPacket, NBirthPacket, NDataPacket, NDeathPacket


class EdgeNode(ABC):

    def __init__(self, group: str, node: str):
        self._group = group
        self._node = node

    @property
    def group(self):
        return self._group

    @property
    def node(self):
        return self._node

    @abstractmethod
    def birth_certificate(self) -> NBirthPacket:
        pass

    def death_certificate(self) -> NDeathPacket:
        packet = NDeathPacket(group=self._group, node=self._node)
        return packet

    @abstractmethod
    def data_packet(self, **kwargs) -> NDataPacket:
        pass


class EdgeDevice(ABC):

    def __init__(self, group: str, node: str, device_id: str):
        self._group = group
        self._node = node
        self._device_id = device_id

    @property
    def group(self):
        return self._group

    @property
    def node(self):
        return self._node

    @property
    def device(self):
        return self._device_id

    @abstractmethod
    def birth_certificate(self) -> DBirthPacket:
        pass

    def death_certificate(self) -> DDeathPacket:
        packet = DDeathPacket(group=self.group, node=self.node, device_id=self.device)
        return packet

    @abstractmethod
    def data_packet(self, **kwargs) -> DDataPacket:
        pass
