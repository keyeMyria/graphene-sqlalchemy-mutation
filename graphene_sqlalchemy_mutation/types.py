from graphene import InputObjectType, ObjectType, String, ID, Field, List, Int
from graphene.types.mutation import MutationMeta, Mutation
from graphene.utils import is_base_type
from graphene.utils.is_base_type import is_base_type
from graphene.types.argument import Argument
from graphene.types.dynamic import Dynamic
from graphene_sqlalchemy.converter import convert_sqlalchemy_column
from graphene_sqlalchemy.utils import get_query, is_mapped
from graphene.types.typemap import TypeMap, NonNull, Interface, Scalar, Enum, Union,  GraphQLTypeMap


from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.inspection import inspect as sqlalchemyinspect

import re
import graphene
import base64


def graphene_reducer(self, map, type):
        if isinstance(type, (List, NonNull)):
            return self.reducer(map, type.of_type)
        if type._meta.name in map:
            _type = map[type._meta.name]
            return map

        if issubclass(type, ObjectType):
            internal_type = self.construct_objecttype(map, type)
        elif issubclass(type, InputObjectType):
            internal_type = self.construct_inputobjecttype(map, type)
        elif issubclass(type, Interface):
            internal_type = self.construct_interface(map, type)
        elif issubclass(type, Scalar):
            internal_type = self.construct_scalar(map, type)
        elif issubclass(type, Enum):
            internal_type = self.construct_enum(map, type)
        elif issubclass(type, Union):
            internal_type = self.construct_union(map, type)
        else:
            raise Exception(
                "Expected Graphene type, but received: {}.".format(type))

        return GraphQLTypeMap.reducer(map, internal_type)


TypeMap.graphene_reducer = graphene_reducer


def camel_to_snake(s):
    s = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', s)
    s = re.sub('(.)([0-9]+)', r'\1_\2', s)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s).lower()


class SQLAlchemyMutationMeta(MutationMeta):

    def __new__(cls, name, bases, attrs):

        if not is_base_type(bases, SQLAlchemyMutationMeta):
            return type.__new__(cls, name, bases, attrs)

        input_class = attrs.pop('Meta', None)

        if not input_class or not getattr(input_class, 'model', None) or \
                not getattr(input_class, 'field', None):
            return MutationMeta.__new__(cls, name, bases, attrs)

        assert is_mapped(input_class.model), ('You need valid SQLAlchemy Model in {}.Meta, received "{}".').format(name, input_class.model)

        field_name = camel_to_snake(input_class.model.__name__)
        inspected_model = sqlalchemyinspect(input_class.model)

        def mutate(cls, instance, args, context, info):
            session = cls.query
            arg_attrs = {}
            primary_key = {}
            for name, column in inspected_model.columns.items():
                if column.primary_key and name in args:
                    try:
                        klazz, pk = base64.b64decode(args['id']).split(b":")
                    except:
                        pk = args.get(name, None)
                    finally:
                        primary_key[name] = int(pk)
                    continue
                if name in args:
                    arg_attrs[name] = args.get(name, None)

            if len(primary_key) > 0:
                 session.query(input_class.model).filter_by(**primary_key).update(arg_attrs)
                 session.commit()
                 field = session.query(input_class.model).filter_by(**primary_key).first()
            else:
                field = input_class.model(**arg_attrs)
                session.add(field)

            try:
                session.commit()
                ok = True
                message = "ok"
            except SQLAlchemyError as e:
                session.rollback()
                message = e.message
                ok = False

            kwargs = {
                'ok': ok,
                'message': message,
                field_name: field
            }
            return cls(**kwargs)

        input_attrs = {}

        for name, column in inspected_model.columns.items():
            input_attrs[name] = convert_sqlalchemy_column(column)
            if column.default or column.server_default or column.primary_key:
                input_attrs[name].kwargs['required'] = False

        mutation_attrs = {
            'Input': type('Input', (object,), input_attrs),
            'ok': graphene.Boolean(),
            'message': graphene.String(),
            'mutate': classmethod(mutate),
            field_name: graphene.Field(input_class.field)
        }

        cls = MutationMeta.__new__(cls, name, bases, mutation_attrs)
        return cls


class SQLAlchemyMutation(Mutation, metaclass=SQLAlchemyMutationMeta):
    pass


class SQLAlchemyMutationMetaUpdate(SQLAlchemyMutationMeta):

    def __new__(cls, name, bases, attrs):

        if not is_base_type(bases, SQLAlchemyMutationMeta):
            return type.__new__(cls, name, bases, attrs)

        input_class = attrs.pop('Meta', None)

        if not input_class or not getattr(input_class, 'model', None) or \
                not getattr(input_class, 'field', None):
            return MutationMeta.__new__(cls, name, bases, attrs)

        assert is_mapped(input_class.model), \
            ('You need valid SQLAlchemy Model in {}.Meta, received "{}".').format(name, input_class.model)

        field_name = camel_to_snake(input_class.model.__name__)
        inspected_model = sqlalchemyinspect(input_class.model)

        def mutate(cls, instance, args, context, info):
            session = cls.query
            arg_attrs = {}
            primary_key = {}
            for name, column in inspected_model.columns.items():
                if column.primary_key and name in args:
                    try:
                        klazz, pk = base64.b64decode(args['id']).split(b":")
                    except:
                        pk = args.get(name, None)
                    finally:
                        primary_key[name] = int(pk)
                    continue
                if name in args:
                    arg_attrs[name] = args.get(name, None)

            if len(primary_key) > 0:
                 session.query(input_class.model).filter_by(**primary_key).update(arg_attrs)
                 session.commit()
                 field = session.query(input_class.model).filter_by(**primary_key).first()
            else:
                field = input_class.model(**arg_attrs)
                session.add(field)

            try:
                session.commit()
                ok = True
                message = "ok"
            except SQLAlchemyError as e:
                session.rollback()
                message = e.message
                ok = False

            kwargs = {
                'ok': ok,
                'message': message,
                field_name: field
            }
            return cls(**kwargs)

        input_attrs = {}
        for name, column in inspected_model.columns.items():
            input_attrs[name] = convert_sqlalchemy_column(column)
            if column.primary_key:
                input_attrs[name].kwargs['required'] = True
            else:
                input_attrs[name].kwargs['required'] = False

        mutation_attrs = {
            'Input': type('Input', (object,), input_attrs),
            'ok': graphene.Boolean(),
            'message': graphene.String(),
            'mutate': classmethod(mutate),
            field_name: graphene.Field(input_class.field)
        }

        cls = MutationMeta.__new__(cls, name, bases, mutation_attrs)
        return cls


class SQLAlchemyMutationUpdate(Mutation, metaclass=SQLAlchemyMutationMetaUpdate):
    pass


class SQLAlchemyMutationMetaDelete(SQLAlchemyMutationMeta):

    def __new__(cls, name, bases, attrs):

        if not is_base_type(bases, SQLAlchemyMutationMeta):
            return type.__new__(cls, name, bases, attrs)

        input_class = attrs.pop('Meta', None)

        if not input_class or not getattr(input_class, 'model', None) or \
                not getattr(input_class, 'field', None):
            return MutationMeta.__new__(cls, name, bases, attrs)

        assert is_mapped(input_class.model), \
            ('You need valid SQLAlchemy Model in {}.Meta, received "{}".').format(name, input_class.model)

        field_name = camel_to_snake(input_class.model.__name__)
        inspected_model = sqlalchemyinspect(input_class.model)

        def mutate(cls, instance, args, context, info):
            session = cls.query
            arg_attrs = {}
            primary_key = {}
            for name, column in inspected_model.columns.items():
                if column.primary_key and name in args:
                    try:
                        klazz, pk = base64.b64decode(args['id']).split(b":")
                    except:
                        pk = args.get(name, None)
                    finally:
                        primary_key[name] = int(pk)
                    break

            session.query(input_class.model).filter_by(**primary_key).delete()

            try:
                session.commit()
                ok = True
                message = "ok"
            except SQLAlchemyError as e:
                session.rollback()
                message = e.message
                ok = False

            kwargs = {
                'ok': ok,
                'message': message,
            }
            return cls(**kwargs)

        input_attrs = {}
        for name, column in inspected_model.columns.items():
            if column.primary_key:
                input_attrs[name] = convert_sqlalchemy_column(column)
                input_attrs[name].kwargs['required'] = True
                break

        mutation_attrs = {
            'Input': type('Input', (object, ), input_attrs),
            'ok': graphene.Boolean(),
            'message': graphene.String(),
            'mutate': classmethod(mutate),
            field_name: graphene.Field(input_class.field)
        }

        cls = MutationMeta.__new__(cls, name, bases, mutation_attrs)
        return cls


class SQLAlchemyMutationDelete(Mutation, metaclass=SQLAlchemyMutationMetaDelete):
    pass



