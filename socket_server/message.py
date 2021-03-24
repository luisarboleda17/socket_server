
import json


class Message:
    PROTOCOL_HEADER_LENGTH = 4
    HEADER_CONTENT_TYPE_KEY = 'content-type'
    HEADER_CONTENT_LENGTH_KEY = 'content-length'

    def __init__(self, content_type: str, message=None):
        self._content_type = content_type
        self._message = message

    @property
    def header(self):
        encoded_message_length = self.encoded_message_length
        return {
            Message.HEADER_CONTENT_TYPE_KEY: self._content_type,
            Message.HEADER_CONTENT_LENGTH_KEY: encoded_message_length if encoded_message_length else 0
        }

    @property
    def encoded_header(self):
        encoded_header, _ = Message.encode_dict(self.header)
        return encoded_header

    @property
    def encoded_header_length(self):
        return len(self.encoded_header)

    @property
    def message(self):
        return self._message

    @message.setter
    def message(self, message):
        self._message = message

    @property
    def encoded_message(self):
        return self._message

    @property
    def encoded_message_length(self):
        if not self.encoded_message:
            return
        return len(self.encoded_message)

    def __repr__(self):
        return f'Message({self._message})'

    @staticmethod
    def encode_text(text: str):
        if not text:
            return None, 0
        encoded_text = text.encode('utf-8')
        return encoded_text, len(encoded_text)

    @staticmethod
    def encode_dict(value: dict):
        if not value:
            return None, 0
        encoded_value = json.dumps(value).encode('utf-8')
        return encoded_value, len(encoded_value)

    @staticmethod
    def decode_text(b_text: bytes):
        return b_text.decode('utf-8')

    @staticmethod
    def decode_dict(b_value: bytes):
        decoded = json.loads(b_value.decode('utf-8'))
        return decoded

    def to_message(self, message_class):
        message = message_class()
        message.set_message_from_bytes(self.message)
        return message

    def set_header_from_bytes(self, b_header: bytes):
        decoded_header = Message.decode_dict(b_header)
        self._content_type = decoded_header[Message.HEADER_CONTENT_TYPE_KEY]

    def set_message_from_bytes(self, b_message: bytes):
        self._message = b_message

    def _encode_header_length(self):
        encoded_length = self.encoded_header_length

        if not encoded_length:
            return

        return str(encoded_length).zfill(Message.PROTOCOL_HEADER_LENGTH).encode('utf-8')

    def encode(self):
        encoded_header_length = self._encode_header_length()
        encoded_header = self.encoded_header
        encoded_message = self.encoded_message

        if not encoded_header_length or not encoded_header or not encoded_message:
            return
        return encoded_header_length + encoded_header + encoded_message


class TextMessage(Message):
    @property
    def encoded_message(self):
        encoded_text, _ = TextMessage.encode_text(self._message)
        return encoded_text

    def __init__(self, text_message=None):
        super().__init__(content_type='text/plain', message=text_message)

    def __repr__(self):
        return f'TextMessage({self._message})'

    @staticmethod
    def from_message(message: Message):
        return message.to_message(TextMessage)

    def set_message_from_bytes(self, b_message: bytes):
        if not b_message:
            self._message = None
            return
        decoded_text = TextMessage.decode_text(b_message)
        self._message = decoded_text


class CloseMessage(TextMessage):
    CLOSE_CONTENT = '---CLOSE---'

    def __init__(self):
        super().__init__(CloseMessage.CLOSE_CONTENT)


class JSONMessage(Message):
    @property
    def encoded_message(self):
        encoded_json, _ = TextMessage.encode_dict(self._message)
        return encoded_json

    def __init__(self, json_message=None):
        super().__init__(content_type='application/json', message=json_message)

    def __repr__(self):
        return f'JSONMessage({self._message})'

    @staticmethod
    def from_message(message: Message):
        return message.to_message(JSONMessage)

    def set_message_from_bytes(self, b_message: bytes):
        if not b_message:
            self._message = None
            return

        decoded_json = JSONMessage.decode_dict(b_message)
        self._message = decoded_json


class EventMessage(JSONMessage):
    EVENT_NAME_KEY = 'event_name'
    EVENT_MESSAGE_KEY = 'event_message'

    @property
    def event_name(self):
        return self._message[EventMessage.EVENT_NAME_KEY]

    @property
    def event_message(self):
        return self._message[EventMessage.EVENT_MESSAGE_KEY]

    def __init__(self, name, message):
        super().__init__({EventMessage.EVENT_NAME_KEY: name, EventMessage.EVENT_MESSAGE_KEY: message})

    def __repr__(self):
        return f'EventMessage({self.event_name}, {self.event_message})'

    @staticmethod
    def from_message(message: Message):
        return message.to_message(EventMessage)

    @staticmethod
    def from_json_message(json_message: JSONMessage):
        message_content = json_message.message
        return EventMessage(
            message_content[EventMessage.EVENT_NAME_KEY],
            message_content[EventMessage.EVENT_MESSAGE_KEY]
        )

    @staticmethod
    def is_event_message(value: dict):
        return EventMessage.EVENT_NAME_KEY in value and EventMessage.EVENT_MESSAGE_KEY in value


class MessageParser:
    def __init__(self):
        self._data = b''
        self._current_header_length = None
        self._current_header = None

    def received_data(self, data):
        if data and len(data) > 0:
            self._data += data

    def _read_protocol_header(self):
        if not len(self._data) >= Message.PROTOCOL_HEADER_LENGTH:
            return

        try:
            header_length = int(self._data[:Message.PROTOCOL_HEADER_LENGTH].decode('utf-8'))
            self._current_header_length = header_length
            self._data = self._data[Message.PROTOCOL_HEADER_LENGTH:]
            return header_length
        except ValueError:
            return

    def _read_message_header(self):
        if not self._current_header_length:
            return

        if not len(self._data) >= self._current_header_length:
            return

        try:
            self._current_header = Message.decode_dict(self._data[:self._current_header_length])
            self._data = self._data[self._current_header_length:]
            return self._current_header
        except json.JSONDecoder:
            return

    def _map_message_by_type(self, message_content: bytes):
        if not self._current_header:
            raise AssertionError('Cannot find message header')

        content_type = self._current_header[Message.HEADER_CONTENT_TYPE_KEY]
        message = Message(content_type, message_content)

        if content_type == 'text/plain':
            return TextMessage.from_message(message)
        elif content_type == 'application/json':
            json_message = JSONMessage.from_message(message)

            if EventMessage.is_event_message(json_message.message):
                return EventMessage.from_json_message(json_message)
            else:
                return json_message
        else:
            return message

    def _read_message_content(self):
        if not self._current_header:
            return
        if not (Message.HEADER_CONTENT_TYPE_KEY in self._current_header) or not (Message.HEADER_CONTENT_LENGTH_KEY in self._current_header):
            return

        if not len(self._data) >= self._current_header[Message.HEADER_CONTENT_LENGTH_KEY]:
            return

        try:
            message_length = self._current_header[Message.HEADER_CONTENT_LENGTH_KEY]
            message_content = self._data[:message_length]
            mapped_message = self._map_message_by_type(message_content)
            self._data = self._data[message_length:]
            self._current_header_length = None
            self._current_header = None
            return mapped_message
        except json.JSONDecodeError:
            return

    def parse_messages(self):
        if not len(self._data) > 0:
            return

        data_available = True

        while data_available:
            if not self._current_header_length:
                self._read_protocol_header()

            if not self._current_header:
                self._read_message_header()

            message = self._read_message_content()

            if message:
                yield message
            else:
                data_available = False
