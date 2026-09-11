"""The authentication methods a database connection can use."""

IAM_AUTH_METHOD = "iam"


def uses_iam_auth(credentials_extra) -> bool:
    """Tells if a connection must authenticate with an IAM token."""
    if not credentials_extra:
        return False
    return credentials_extra.get("auth_method") == IAM_AUTH_METHOD
