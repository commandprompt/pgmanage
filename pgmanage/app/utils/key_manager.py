class KeyManager:

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

key_manager = KeyManager()
