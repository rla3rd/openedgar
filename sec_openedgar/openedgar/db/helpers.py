"""
MIT License
Copyright (c) 2024 Richard Albright
Copyright (c) 2018 ContraxSuite, LLC

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
"""

from contextlib import contextmanager
from sqlalchemy import or_, and_, not_, desc, asc, func
from sqlalchemy.orm import joinedload, selectinload, contains_eager

from .session import Session
from .models import *

# Context manager for database sessions
@contextmanager
def session_scope():
    """Provide a transactional scope around a series of operations."""
    session = Session()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


# Helper functions to replace common Django ORM operations

def get_or_create(session, model, **kwargs):
    """
    Get or create a model instance.
    Similar to Django's get_or_create() method.
    
    Args:
        session: SQLAlchemy session
        model: SQLAlchemy model class
        **kwargs: Attributes to filter by and use for creation
        
    Returns:
        tuple: (instance, created) where created is a boolean
    """
    instance = session.query(model).filter_by(**kwargs).first()
    if instance:
        return instance, False
    else:
        instance = model(**kwargs)
        session.add(instance)
        session.flush()  # Flush to get the ID without committing
        return instance, True


def update_or_create(session, model, defaults=None, **kwargs):
    """
    Update or create a model instance.
    Similar to Django's update_or_create() method.
    
    Args:
        session: SQLAlchemy session
        model: SQLAlchemy model class
        defaults: Dict of attributes to update if object exists
        **kwargs: Attributes to filter by and use for creation
        
    Returns:
        tuple: (instance, created) where created is a boolean
    """
    defaults = defaults or {}
    instance = session.query(model).filter_by(**kwargs).first()
    if instance:
        for key, value in defaults.items():
            setattr(instance, key, value)
        return instance, False
    else:
        kwargs.update(defaults)
        instance = model(**kwargs)
        session.add(instance)
        session.flush()  # Flush to get the ID without committing
        return instance, True


def bulk_create(session, model, objects):
    """
    Bulk create model instances.
    Similar to Django's bulk_create() method.
    
    Args:
        session: SQLAlchemy session
        model: SQLAlchemy model class
        objects: List of model instances
        
    Returns:
        list: Created objects
    """
    session.add_all(objects)
    session.flush()  # Flush to get the IDs without committing
    return objects


def filter_queryset(session, model, **kwargs):
    """
    Filter a queryset by keyword arguments.
    Similar to Django's filter() method.
    
    Args:
        session: SQLAlchemy session
        model: SQLAlchemy model class
        **kwargs: Attributes to filter by
        
    Returns:
        Query: SQLAlchemy query object
    """
    query = session.query(model)
    
    for key, value in kwargs.items():
        # Handle special operators like __in, __contains, etc.
        if '__' in key:
            field, operator = key.split('__', 1)
            column = getattr(model, field)
            
            if operator == 'in':
                query = query.filter(column.in_(value))
            elif operator == 'contains':
                query = query.filter(column.contains(value))
            elif operator == 'startswith':
                query = query.filter(column.startswith(value))
            elif operator == 'endswith':
                query = query.filter(column.endswith(value))
            elif operator == 'isnull':
                if value:
                    query = query.filter(column.is_(None))
                else:
                    query = query.filter(column.isnot(None))
            elif operator == 'gt':
                query = query.filter(column > value)
            elif operator == 'gte':
                query = query.filter(column >= value)
            elif operator == 'lt':
                query = query.filter(column < value)
            elif operator == 'lte':
                query = query.filter(column <= value)
            elif operator == 'exact':
                query = query.filter(column == value)
            elif operator == 'iexact':
                query = query.filter(func.lower(column) == func.lower(value))
        else:
            query = query.filter(getattr(model, key) == value)
    
    return query


def exclude_queryset(session, model, **kwargs):
    """
    Exclude objects from a queryset by keyword arguments.
    Similar to Django's exclude() method.
    
    Args:
        session: SQLAlchemy session
        model: SQLAlchemy model class
        **kwargs: Attributes to exclude by
        
    Returns:
        Query: SQLAlchemy query object
    """
    query = session.query(model)
    
    for key, value in kwargs.items():
        # Handle special operators like __in, __contains, etc.
        if '__' in key:
            field, operator = key.split('__', 1)
            column = getattr(model, field)
            
            if operator == 'in':
                query = query.filter(~column.in_(value))
            elif operator == 'contains':
                query = query.filter(~column.contains(value))
            elif operator == 'startswith':
                query = query.filter(~column.startswith(value))
            elif operator == 'endswith':
                query = query.filter(~column.endswith(value))
            elif operator == 'isnull':
                if value:
                    query = query.filter(column.isnot(None))
                else:
                    query = query.filter(column.is_(None))
            elif operator == 'gt':
                query = query.filter(column <= value)
            elif operator == 'gte':
                query = query.filter(column < value)
            elif operator == 'lt':
                query = query.filter(column >= value)
            elif operator == 'lte':
                query = query.filter(column > value)
            elif operator == 'exact':
                query = query.filter(column != value)
            elif operator == 'iexact':
                query = query.filter(func.lower(column) != func.lower(value))
        else:
            query = query.filter(getattr(model, key) != value)
    
    return query


def order_by_queryset(query, *fields):
    """
    Order a queryset by fields.
    Similar to Django's order_by() method.
    
    Args:
        query: SQLAlchemy query object
        *fields: Fields to order by (prefix with - for descending)
        
    Returns:
        Query: SQLAlchemy query object
    """
    model_class = query.column_descriptions[0]['entity']
    
    for field in fields:
        if field.startswith('-'):
            field_name = field[1:]
            column = getattr(model_class, field_name)
            query = query.order_by(desc(column))
        else:
            column = getattr(model_class, field)
            query = query.order_by(asc(column))
    
    return query


def get_or_404(session, model, **kwargs):
    """
    Get an object or raise a 404 exception.
    Similar to Django's get_or_404() function.
    
    Args:
        session: SQLAlchemy session
        model: SQLAlchemy model class
        **kwargs: Attributes to filter by
        
    Returns:
        Model instance
        
    Raises:
        HTTPException: 404 Not Found
    """
    instance = session.query(model).filter_by(**kwargs).first()
    if instance is None:
        from fastapi import HTTPException
        raise HTTPException(status_code=404, detail=f"{model.__name__} not found")
    return instance


def count_queryset(query):
    """
    Count the number of objects in a queryset.
    Similar to Django's count() method.
    
    Args:
        query: SQLAlchemy query object
        
    Returns:
        int: Count of objects
    """
    return query.count()


def exists_queryset(query):
    """
    Check if a queryset contains any objects.
    Similar to Django's exists() method.
    
    Args:
        query: SQLAlchemy query object
        
    Returns:
        bool: True if the queryset contains objects, False otherwise
    """
    return query.session.query(query.exists()).scalar()


def values_queryset(query, *fields):
    """
    Return a list of dictionaries containing the values for the specified fields.
    Similar to Django's values() method.
    
    Args:
        query: SQLAlchemy query object
        *fields: Fields to include in the result
        
    Returns:
        list: List of dictionaries
    """
    result = []
    for row in query:
        item = {}
        for field in fields:
            item[field] = getattr(row, field)
        result.append(item)
    return result


def values_list_queryset(query, *fields, flat=False):
    """
    Return a list of tuples containing the values for the specified fields.
    Similar to Django's values_list() method.
    
    Args:
        query: SQLAlchemy query object
        *fields: Fields to include in the result
        flat: If True and only one field is specified, return a flat list
        
    Returns:
        list: List of tuples or flat list
    """
    if flat and len(fields) != 1:
        raise ValueError("'flat' is not valid when values_list is called with more than one field")
    
    result = []
    for row in query:
        if flat:
            result.append(getattr(row, fields[0]))
        else:
            item = tuple(getattr(row, field) for field in fields)
            result.append(item)
    return result


def annotate_queryset(session, model, **annotations):
    """
    Add annotations to a queryset.
    Similar to Django's annotate() method.
    
    Args:
        session: SQLAlchemy session
        model: SQLAlchemy model class
        **annotations: Annotations to add
        
    Returns:
        Query: SQLAlchemy query object
    """
    query = session.query(model)
    
    for name, expression in annotations.items():
        query = query.add_columns(expression.label(name))
    
    return query


def prefetch_related(query, *related_names):
    """
    Prefetch related objects.
    Similar to Django's prefetch_related() method.
    
    Args:
        query: SQLAlchemy query object
        *related_names: Names of related objects to prefetch
        
    Returns:
        Query: SQLAlchemy query object
    """
    for related_name in related_names:
        query = query.options(selectinload(related_name))
    
    return query


def select_related(query, *related_names):
    """
    Select related objects.
    Similar to Django's select_related() method.
    
    Args:
        query: SQLAlchemy query object
        *related_names: Names of related objects to select
        
    Returns:
        Query: SQLAlchemy query object
    """
    for related_name in related_names:
        query = query.options(joinedload(related_name))
    
    return query
