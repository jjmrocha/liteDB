from litedb.models import Type, Field


class String(Field):
    data_type = Type.STRING


class Integer(Field):
    data_type = Type.INTEGER


class Float(Field):
    data_type = Type.FLOAT


class Boolean(Field):
    data_type = Type.BOOLEAN

