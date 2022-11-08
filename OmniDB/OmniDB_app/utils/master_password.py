from OmniDB_app.utils.crypto import decrypt, encrypt


MASTERPASS_CHECK_TEXT = 'Knowledge is power'


class MasterPassManager:

    __users = dict()

    def get(self, current_user):
        user = self.__users.get(current_user.id, None)
        if user is not None:
            return user.get('key', None)
        return None


    def set(self, current_user, _key):
        user = self.__users.get(current_user.id, None)
        if user is None:
            self.__users[current_user.id] = dict(key = _key)
        else:
            user['key'] = _key

    def remove(self, current_user):
        user = self.__users.get(current_user.id, None)
        if user is not None:
            del self.__users[current_user.id]

    @staticmethod
    def validate_master_password(current_user, password):
        """
        Validate the password/key against the stored encrypted text
        :param current_user: current user instance
        :param password: password/key
        :return: Valid or not
        """
        # master pass is incorrect if decryption fails
        try:
            decrypted_text = decrypt(current_user.masterpass_check, password)

            if isinstance(decrypted_text, bytes):
                decrypted_text = decrypted_text.decode()

            if MASTERPASS_CHECK_TEXT != decrypted_text:
                return False
            else:
                return True
        except Exception:
            False

    @staticmethod
    def set_masterpass_check_text(current_user, password):
        """
        Set the encrypted text which will be used later to validate entered key
        :param current_user: current user instance
        :param password: key
        """
        try:
            masterpass_check = encrypt(MASTERPASS_CHECK_TEXT, password)


            # set the encrypted sample text with the new
            # master pass

            current_user.masterpass_check = masterpass_check
            current_user.save()

        except Exception:
            raise


master_pass_manager = MasterPassManager()
