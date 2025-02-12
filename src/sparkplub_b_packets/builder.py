from abc import ABC, abstractmethod

from sparkplub_b_packets.packets import DBirthPacket, DCmdPacket, DDataPacket, DDeathPacket, NBirthPacket, NCmdPacket, NDataPacket, NDeathPacket


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

    def node_command(self) -> NCmdPacket:
        packet = NCmdPacket(group=self._group, node=self._node)
        return packet

    @abstractmethod
    def birth_certificate(self) -> NBirthPacket:
        raise NotImplementedError()

    def death_certificate(self) -> NDeathPacket:
        packet = NDeathPacket(group=self._group, node=self._node)
        return packet

    @abstractmethod
    def data_packet(self, **kwargs) -> NDataPacket:
        raise NotImplementedError()


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

    def device_command(self) -> DCmdPacket:
        packet = DCmdPacket(group=self._group, node=self._node)
        return packet

    @abstractmethod
    def birth_certificate(self) -> DBirthPacket:
        raise NotImplementedError()

    def death_certificate(self) -> DDeathPacket:
        packet = DDeathPacket(group=self.group, node=self.node, device_id=self.device)
        return packet

    @abstractmethod
    def data_packet(self, **kwargs) -> DDataPacket:
        raise NotImplementedError()
