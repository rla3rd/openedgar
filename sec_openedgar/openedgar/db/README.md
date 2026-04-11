# Django ORM to SQLAlchemy Migration

This directory contains the SQLAlchemy implementation that replaces the Django ORM in the OpenEdgar project.

## Structure

- `__init__.py`: Exports the main SQLAlchemy components
- `session.py`: Configures the SQLAlchemy engine, session, and base class
- `models.py`: Contains all SQLAlchemy model definitions
- `utils.py`: Utilities for creating tables and migrating data
- `helpers.py`: Helper functions to ease transition from Django ORM to SQLAlchemy

## Migration Process

### 1. Create SQLAlchemy Tables

To create all the SQLAlchemy tables in your database:

```bash
python -m openedgar.db.utils --create
```

### 2. Migrate Data from Django ORM

To migrate existing data from Django ORM to SQLAlchemy:

```bash
python -m openedgar.db.utils --migrate
```

### 3. Check Migration Status

To verify that all tables have been created:

```bash
python -m openedgar.db.utils --check
```

## Using SQLAlchemy in Your Code

### Basic Usage

```python
from openedgar.db import Session
from openedgar.db.models import Company, Filing

# Create a session
session = Session()

try:
    # Query data
    companies = session.query(Company).all()
    
    # Create a new record
    new_company = Company(cik=1234567890, cik_name="Example Company")
    session.add(new_company)
    
    # Commit changes
    session.commit()
finally:
    # Always close the session
    session.close()
```

### Using the Session Context Manager

```python
from openedgar.db.helpers import session_scope
from openedgar.db.models import Company

# Use the context manager to handle session lifecycle
with session_scope() as session:
    companies = session.query(Company).all()
    new_company = Company(cik=1234567890, cik_name="Example Company")
    session.add(new_company)
    # No need to commit or close - handled by context manager
```

### Helper Functions (Django ORM-like API)

The `helpers.py` module provides functions that mimic Django ORM methods:

```python
from openedgar.db.helpers import (
    session_scope, get_or_create, update_or_create,
    filter_queryset, exclude_queryset, order_by_queryset
)
from openedgar.db.models import Company

# Example: get_or_create
with session_scope() as session:
    company, created = get_or_create(session, Company, cik=1234567890, cik_name="Example Company")
    if created:
        print("Created new company")
    else:
        print("Found existing company")

# Example: filter with conditions
with session_scope() as session:
    companies = filter_queryset(session, Company, cik_name__contains="Corp")
    for company in companies:
        print(company.cik, company.cik_name)
```

## Differences from Django ORM

### Key Differences

1. **Session Management**: SQLAlchemy requires explicit session management
2. **Query Execution**: Django queries are lazy, SQLAlchemy queries execute when iterated
3. **Relationships**: SQLAlchemy uses relationship() instead of ForeignKey fields
4. **Migrations**: No built-in migrations like Django (use Alembic if needed)

### Common Django ORM to SQLAlchemy Mappings

| Django ORM | SQLAlchemy |
|------------|------------|
| `Model.objects.all()` | `session.query(Model).all()` |
| `Model.objects.filter(field=value)` | `session.query(Model).filter_by(field=value)` |
| `Model.objects.get(pk=1)` | `session.query(Model).get(1)` |
| `instance.save()` | `session.add(instance); session.commit()` |
| `Model.objects.create(**kwargs)` | `session.add(Model(**kwargs)); session.commit()` |
| `instance.delete()` | `session.delete(instance); session.commit()` |
| `Model.objects.filter(field__in=[1,2,3])` | `session.query(Model).filter(Model.field.in_([1,2,3]))` |
| `Model.objects.order_by('field')` | `session.query(Model).order_by(Model.field)` |
| `Model.objects.values('field1', 'field2')` | `session.query(Model.field1, Model.field2)` |

## Additional Resources

- [SQLAlchemy Documentation](https://docs.sqlalchemy.org/)
- [SQLAlchemy ORM Tutorial](https://docs.sqlalchemy.org/en/14/orm/tutorial.html)
- [Alembic (for migrations)](https://alembic.sqlalchemy.org/)
