import os
import shutil

from app.models.main import UserDetails


def get_utility_path(utility, current_user):
    user_d = UserDetails.objects.get(user=current_user)

    if not user_d.binary_path:
        utility_path = shutil.which(utility)
    else:
        utility_path = os.path.join(
            user_d.binary_path, utility if os.name != "nt" else (utility + ".exe")
        )

    if utility_path is None or not os.path.exists(utility_path):
        raise FileNotFoundError
    return utility_path
