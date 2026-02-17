# Security Policy

## Supported Versions

| Version | Supported          |
| ------- | ------------------ |
| 0.1.x   | ✅                 |

## Security Overview

YesDB is designed for **single-user, trusted environments**. It includes basic security features but is not suitable for untrusted or multi-user scenarios without additional application-level controls.

### Built-in Security Features

- ✅ Path validation (prevents access to system files)
- ✅ Resource limits (SQL length, record size, database size)
- ✅ Input validation (table and column names)
- ✅ Error sanitization in production mode

### Known Limitations

- ⚠️ No encryption at rest (data stored in plaintext)
- ⚠️ No user authentication or access control
- ⚠️ No concurrent access protection
- ⚠️ Single-user, single-process design

## Recommended Use

**Safe for:**
- Local development
- Single-user desktop apps
- Prototyping and testing

**Not recommended for:**
- Multi-user web applications
- Sensitive data without encryption
- Untrusted environments
- Concurrent access scenarios

## Best Practices

```python
from chidb import YesDB

# ✅ Good: Use in trusted environment
db = YesDB('my_local_data.db')

# ✅ Good: Validate input
safe_value = user_input.replace("'", "''")

# ❌ Avoid: Untrusted file paths
# ❌ Avoid: Storing passwords in plaintext
```

## Reporting Vulnerabilities

If you discover a security vulnerability:

1. **Do not** open a public issue
2. Email: security@yourdomain.com (update with your email)
3. Include:
   - Description of the issue
   - Steps to reproduce
   - Potential impact

**Response timeline:**
- 24 hours: Acknowledgment
- 7 days: Assessment
- 30 days: Fix and release (if confirmed)

## Version History

- **0.1.0**: Initial release with basic security features

---

**Disclaimer**: YesDB is provided "as is" for educational purposes. For production systems with security requirements, use established databases like PostgreSQL, MySQL, or SQLite.
