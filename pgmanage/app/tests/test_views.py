from collections import OrderedDict

from django.contrib.auth.models import User
from django.test import TestCase
from django.urls import reverse

from pgmanage import settings

from .utils_testing import (USERS, build_client_ajax_request,
                            execute_client_login,
                            get_client_ajax_response_content,
                            get_client_omnidb_session,
                            get_session_alert_message)


class ConnectionsNoSession(TestCase):
    """Test views from connections.py file with no user session.
    """

    pass


class ConnectionsSession(TestCase):
    """Test views from connections.py file with user session.
    """

    pass


class LoginNoSession(TestCase):
    """Test views from login.py file with no user session.
    """

    def setUp(self):
        """Used to setup common properties between tests in this class.
        """

        self.user = {
            'user': USERS['ADMIN']['USER'],
            'password': USERS['ADMIN']['PASSWORD']
        }

        self.assertIsNone(get_client_omnidb_session(p_client=self.client))


    def test_get_index_user_pwd(self):
        """Test if is redirected to workspace when providing valid user and password parameters.
        """

        response = self.client.get(
            reverse('login'),
            {
                'user': self.user['user'],
                'pwd': self.user['password']
            },
            follow=True
        )

        self.assertEqual(response.status_code, 200)
        self.assertIn('app/workspace.html', [template.name for template in response.templates])
        self.assertEqual(len(response.redirect_chain), 2)
        self.assertEqual(response.redirect_chain[-1][0], reverse('workspace'))
        self.assertEqual(response.redirect_chain[0][1], 302)
        self.assertIn('pgmanage_short_version', response.context)
        self.assertEqual(response.context['pgmanage_short_version'], settings.PGMANAGE_SHORT_VERSION)


    def test_get_index_no_user_pwd(self):
        """Test if is redirected to workspace when providing invalid user and valid password parameters.
        """

        response = self.client.get(
            reverse('login'),
            {
                'user': '{p_user}kkk'.format(p_user=self.user['user']),
                'pwd': self.user['password']
            },
            follow=True
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(len(response.redirect_chain), 0)
        self.assertEqual(response.content, b'INVALID APP TOKEN')


    def test_get_index_user_no_pwd(self):
        """Test if is redirected to workspace when providing valid user and invalid password parameters.
        """

        response = self.client.get(
            reverse('login'),
            {
                'user': self.user['user'],
                'pwd': '{p_password}kkk'.format(p_password=self.user['password'])
            },
            follow=True
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(len(response.redirect_chain), 0)
        self.assertEqual(response.content, b'INVALID APP TOKEN')


    def test_get_logout(self):
        """Test if errors out when logging out without loggin in.
        """

        response = self.client.get(
            reverse('logout'),
            follow=True
        )

        self.assertEqual(response.status_code, 401)

    def test_sign_in_no_user_password(self):
        """Test if sign in fails with invalid user and valid password.
        """

        response = self.client.post(
            reverse('sign_in'),
            build_client_ajax_request(
                p_data={
                    'username': '{p_user}kkk'.format(p_user=self.user['user']),
                    'password': self.user['password']
                }
            )
        )

        self.assertEqual(response.status_code, 200)
        v_content = get_client_ajax_response_content(p_response=response)
        self.assertEqual(v_content['data'], -1)
        self.assertIsNone(get_client_omnidb_session(p_client=self.client))


    def test_sign_in_user_password(self):
        """Test if sign in succeeds with valid user and valid password.
        """

        response = self.client.post(
            reverse('sign_in'),
            build_client_ajax_request(
                p_data={
                    'username': self.user['user'],
                    'password': self.user['password']
                }
            )
        )

        self.assertEqual(response.status_code, 200)
        v_content = get_client_ajax_response_content(p_response=response)
        self.assertTrue(v_content['data'] >= 0)



class LoginSession(TestCase):
    """Test views from login.py file with user session.
    """

    def setUp(self):
        """Used to setup common properties between tests in this class.
        """

        self.user = {
            'user': USERS['ADMIN']['USER'],
            'password': USERS['ADMIN']['PASSWORD']
        }


        self.assertIsNone(get_client_omnidb_session(p_client=self.client))
        v_successfull, response = execute_client_login(p_client=self.client, p_username=self.user['user'], p_password=self.user['password'])
        self.assertTrue(v_successfull)
        self.assertIsNotNone(get_client_omnidb_session(p_client=self.client))


    def test_get_index_user_pwd(self):
        """Test if is redirected to workspace when providing valid user and password parameters.
        """

        response = self.client.get(
            reverse('login'),
            {
                'user': self.user['user'],
                'pwd': self.user['password']
            },
            follow=True
        )

        self.assertEqual(response.status_code, 200)
        self.assertIn('app/workspace.html', [template.name for template in response.templates])
        self.assertEqual(len(response.redirect_chain), 2)
        self.assertEqual(response.redirect_chain[1][0], reverse('workspace'))
        self.assertEqual(response.redirect_chain[1][1], 302)
        self.assertIn('pgmanage_short_version', response.context)
        self.assertEqual(response.context['pgmanage_short_version'], settings.PGMANAGE_SHORT_VERSION)


    def test_get_index_no_user_pwd(self):
        """Test if is redirected to workspace when providing invalid user and valid password parameters.
        """

        response = self.client.get(
            reverse('login'),
            {
                'user': '{p_user}kkk'.format(p_user=self.user['user']),
                'pwd': self.user['password']
            },
            follow=True
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(len(response.redirect_chain), 0)
        self.assertEqual(response.content, b'INVALID APP TOKEN')


    def test_get_index_user_no_pwd(self):
        """Test if is redirected to workspace when providing valid user and invalid password parameters.
        """

        response = self.client.get(
            reverse('login'),
            {
                'user': self.user['user'],
                'pwd': '{p_password}kkk'.format(p_password=self.user['password'])
            },
            follow=True
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(len(response.redirect_chain), 0)
        self.assertEqual(response.content, b'INVALID APP TOKEN')


    def test_get_logout(self):
        """Test if receives expected response while trying to logout.
        """

        response = self.client.get(
            reverse('logout'),
            follow=True
        )

        self.assertEqual(response.status_code, 200)
        self.assertIn('app/login.html', [template.name for template in response.templates])
        self.assertEqual(len(response.redirect_chain), 2)
        self.assertEqual(response.redirect_chain[-1][0], reverse('login'))
        self.assertEqual(response.redirect_chain[0][1], 302)
        self.assertIsNone(get_session_alert_message(p_client=self.client))
        self.assertIsNone(get_client_omnidb_session(p_client=self.client))


    def test_sign_in_no_user_password(self):
        """Test if sign in fails with invalid user and valid password.
        """
        self.client.logout()
        response = self.client.post(
            reverse('sign_in'),
            build_client_ajax_request(
                p_data={
                    'username': '{p_user}kkk'.format(p_user=self.user['user']),
                    'password': self.user['password']
                }
            )
        )

        self.assertEqual(response.status_code, 200)
        v_content = get_client_ajax_response_content(p_response=response)
        self.assertEqual(v_content['data'], -1)
        self.assertIsNone(get_client_omnidb_session(p_client=self.client))



    def test_sign_in_user_password(self):
        """Test if sign in succeeds with valid user and valid password.
        """

        response = self.client.post(
            reverse('sign_in'),
            build_client_ajax_request(
                p_data={
                    'username': self.user['user'],
                    'password': self.user['password']
                }
            )
        )

        self.assertEqual(response.status_code, 200)
        ajax_response = get_client_ajax_response_content(p_response=response)
        self.assertTrue(ajax_response['data'] >= 0)
        app_session = get_client_omnidb_session(p_client=self.client)
        self.assertIsNotNone(app_session)

        user_db = User.objects.get(username=self.user['user'])

        self.assertEqual(app_session.v_user_id, user_db.id)
        self.assertEqual(app_session.v_user_name, user_db.username)
        self.assertEqual(app_session.v_super_user, user_db.is_superuser)
        self.assertIsInstance(app_session.v_database_index, int)
        self.assertTrue(isinstance(app_session.v_databases, OrderedDict) or isinstance(app_session.v_databases, dict))
        self.assertEqual(app_session.v_user_key, self.client.session.session_key)
