"""Tests for server/auth.py"""

import os
import pytest
from server.auth import (
    generate_api_key,
    hash_api_key,
    hash_password,
    verify_password,
    create_user,
    login_user,
    find_user_by_email,
    find_user_by_api_key_hash,
    get_accounts_db,
    close_accounts_db,
    User,
)


@pytest.fixture(autouse=True)
def fresh_accounts_db(tmp_path, monkeypatch):
    """Give each test a fresh accounts database."""
    db_path = str(tmp_path / "accounts.db")

    # Patch the settings object directly (it's already instantiated at import time)
    from server.config import settings
    monkeypatch.setattr(settings, "ACCOUNTS_DB_PATH", db_path)

    # Reset the global _accounts_db so it re-initializes with the new path
    import server.auth as auth_module
    auth_module._accounts_db = None

    yield db_path

    close_accounts_db()


# ── API key helpers ──────────────────────────────────────────────


class TestApiKey:
    def test_generate_starts_with_prefix(self):
        key = generate_api_key()
        assert key.startswith("yesdb_")

    def test_generate_is_long_enough(self):
        key = generate_api_key()
        assert len(key) > 40

    def test_generate_unique(self):
        keys = {generate_api_key() for _ in range(10)}
        assert len(keys) == 10, "Each generated key should be unique"

    def test_hash_is_sha256_hex(self):
        h = hash_api_key("yesdb_testkey")
        assert len(h) == 64
        assert all(c in "0123456789abcdef" for c in h)

    def test_hash_deterministic(self):
        key = generate_api_key()
        assert hash_api_key(key) == hash_api_key(key)

    def test_different_keys_different_hashes(self):
        k1 = generate_api_key()
        k2 = generate_api_key()
        assert hash_api_key(k1) != hash_api_key(k2)


# ── Password helpers ─────────────────────────────────────────────


class TestPassword:
    def test_hash_and_verify_correct(self):
        h = hash_password("secret123")
        assert verify_password("secret123", h)

    def test_reject_wrong_password(self):
        h = hash_password("secret123")
        assert not verify_password("wrong", h)

    def test_different_hashes_for_same_password(self):
        """bcrypt uses random salt, so hashes differ."""
        h1 = hash_password("same")
        h2 = hash_password("same")
        assert h1 != h2
        # But both should verify
        assert verify_password("same", h1)
        assert verify_password("same", h2)


# ── User creation ────────────────────────────────────────────────


class TestCreateUser:
    def test_create_returns_user_and_api_key(self):
        user, api_key = create_user("alice@uni.edu", "pass123", "Alice")
        assert isinstance(user, User)
        assert user.email == "alice@uni.edu"
        assert user.name == "Alice"
        assert api_key.startswith("yesdb_")

    def test_create_without_name(self):
        user, api_key = create_user("bob@uni.edu", "pass123")
        assert user.email == "bob@uni.edu"
        assert user.name is None

    def test_create_generates_user_id(self):
        user, _ = create_user("charlie@uni.edu", "pass123")
        assert len(user.user_id) == 16  # secrets.token_hex(8) = 16 hex chars

    def test_duplicate_email_raises_409(self):
        create_user("dup@uni.edu", "pass123")
        with pytest.raises(Exception) as exc_info:
            create_user("dup@uni.edu", "other")
        assert "409" in str(exc_info.value) or "already registered" in str(exc_info.value).lower()

    def test_api_key_stored_as_hash(self):
        """The raw API key should NOT be stored — only its hash."""
        user, api_key = create_user("stored@uni.edu", "pass123")
        row = find_user_by_email("stored@uni.edu")
        stored_key_hash = row[4]
        assert stored_key_hash != api_key, "Raw key should not be in DB"
        assert stored_key_hash == hash_api_key(api_key), "Hash of key should match"


# ── User lookup ──────────────────────────────────────────────────


class TestFindUser:
    def test_find_by_email_exists(self):
        create_user("find@uni.edu", "pass123", "Finder")
        row = find_user_by_email("find@uni.edu")
        assert row is not None
        assert row[1] == "find@uni.edu"

    def test_find_by_email_not_found(self):
        row = find_user_by_email("nobody@uni.edu")
        assert row is None

    def test_find_by_api_key_hash_exists(self):
        _, api_key = create_user("keyfind@uni.edu", "pass123")
        row = find_user_by_api_key_hash(hash_api_key(api_key))
        assert row is not None
        assert row[1] == "keyfind@uni.edu"

    def test_find_by_api_key_hash_not_found(self):
        row = find_user_by_api_key_hash("0" * 64)
        assert row is None


# ── Login ────────────────────────────────────────────────────────


class TestLogin:
    def test_login_returns_user_and_new_key(self):
        _, old_key = create_user("login@uni.edu", "pass123", "Loginman")
        user, new_key = login_user("login@uni.edu", "pass123")
        assert user.email == "login@uni.edu"
        assert new_key.startswith("yesdb_")
        assert new_key != old_key, "Login should rotate the API key"

    def test_login_invalidates_old_key(self):
        _, old_key = create_user("rotate@uni.edu", "pass123")
        _, new_key = login_user("rotate@uni.edu", "pass123")
        assert find_user_by_api_key_hash(hash_api_key(old_key)) is None
        assert find_user_by_api_key_hash(hash_api_key(new_key)) is not None

    def test_login_wrong_password(self):
        create_user("wrongpw@uni.edu", "pass123")
        with pytest.raises(Exception) as exc_info:
            login_user("wrongpw@uni.edu", "badpass")
        assert "401" in str(exc_info.value)

    def test_login_nonexistent_email(self):
        with pytest.raises(Exception) as exc_info:
            login_user("ghost@uni.edu", "pass123")
        assert "401" in str(exc_info.value)

    def test_login_preserves_user_id(self):
        """User ID should remain the same after login."""
        user1, _ = create_user("sameid@uni.edu", "pass123")
        user2, _ = login_user("sameid@uni.edu", "pass123")
        assert user1.user_id == user2.user_id
