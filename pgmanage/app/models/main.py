from unittest.util import _MAX_LENGTH
from django.db import models
from django.contrib.auth.models import User


class Technology(models.Model):
    name = models.CharField(max_length=50, blank=False)

class UserDetails(models.Model):
    user = models.OneToOneField(User, on_delete=models.CASCADE)
    theme = models.CharField(max_length=50, blank=False,default='light')
    font_size = models.IntegerField(blank=False,default=12)
    csv_encoding = models.CharField(max_length=50, blank=False, default='utf-8')
    csv_delimiter = models.CharField(max_length=10, blank=False, default=';')
    welcome_closed = models.BooleanField(default=False)
    masterpass_check = models.CharField(max_length=256, default='')
    binary_path = models.CharField(max_length=256, null=True)
    date_format = models.CharField(max_length=200, null=True)

class Shortcut(models.Model):
    user = models.ForeignKey(User, on_delete=models.CASCADE, null=True)
    code = models.CharField(max_length=200, blank=False)
    os = models.CharField(max_length=200, blank=False)
    ctrl_pressed = models.BooleanField(default=False)
    shift_pressed = models.BooleanField(default=False)
    alt_pressed = models.BooleanField(default=False)
    meta_pressed = models.BooleanField(default=False)
    key = models.CharField(max_length=200, blank=False)

class Connection(models.Model):
    user = models.ForeignKey(User,on_delete=models.CASCADE)
    technology = models.ForeignKey(Technology,on_delete=models.CASCADE)
    server = models.CharField(max_length=200, blank=False, default='')
    port = models.CharField(max_length=50, blank=False, default='')
    database = models.CharField(max_length=200, blank=False, default='')
    username = models.CharField(max_length=200, blank=False, default='')
    password = models.CharField(max_length=200, blank=False, default='')
    alias = models.CharField(max_length=200, blank=False, default='')
    ssh_server = models.CharField(max_length=200, blank=False, default='')
    ssh_port = models.CharField(max_length=50, blank=False, default='')
    ssh_user = models.CharField(max_length=200, blank=False, default='')
    ssh_password = models.CharField(max_length=200, blank=False, default='')
    ssh_key = models.TextField(blank=False, default='')
    use_tunnel = models.BooleanField(default=False)
    conn_string = models.TextField(blank=False, default='')
    public = models.BooleanField(default=False)
    connection_params = models.JSONField(default=dict)

class SnippetFolder(models.Model):
    user = models.ForeignKey(User,on_delete=models.CASCADE)
    parent = models.ForeignKey('self',on_delete=models.CASCADE, null=True)
    name = models.CharField(max_length=200, blank=False, default='')
    create_date = models.DateTimeField()
    modify_date = models.DateTimeField()

class SnippetFile(models.Model):
    user = models.ForeignKey(User,on_delete=models.CASCADE)
    parent = models.ForeignKey(SnippetFolder,on_delete=models.CASCADE, null=True)
    name = models.CharField(max_length=200, blank=False, default='')
    create_date = models.DateTimeField()
    modify_date = models.DateTimeField()
    text = models.TextField(blank=False, default='')


class Tab(models.Model):
    user = models.ForeignKey(User,on_delete=models.CASCADE)
    connection = models.ForeignKey(Connection,on_delete=models.CASCADE)
    title = models.CharField(max_length=200, blank=False, default='')
    snippet = models.TextField(blank=False, default='')

class QueryHistory(models.Model):
    user = models.ForeignKey(User,on_delete=models.CASCADE)
    connection = models.ForeignKey(Connection,on_delete=models.CASCADE)
    start_time = models.DateTimeField()
    end_time = models.DateTimeField()
    duration = models.TextField(blank=False, default='')
    status = models.TextField(blank=False, default='')
    snippet = models.TextField(blank=False, default='')

class ConsoleHistory(models.Model):
    user = models.ForeignKey(User,on_delete=models.CASCADE)
    connection = models.ForeignKey(Connection,on_delete=models.CASCADE)
    start_time = models.DateTimeField()
    snippet = models.TextField(blank=False, default='')

class Group(models.Model):
    user = models.ForeignKey(User,on_delete=models.CASCADE)
    name = models.CharField(max_length=50, blank=False, default = '')

class GroupConnection(models.Model):
    group = models.ForeignKey(Group,on_delete=models.CASCADE)
    connection = models.ForeignKey(Connection,on_delete=models.CASCADE)

    class Meta:
        constraints = [
            models.UniqueConstraint(fields=['group', 'connection'], name='unique_group_connection')
        ]

class MonUnits(models.Model):
    user = models.ForeignKey(User,on_delete=models.CASCADE, null=True)
    technology = models.ForeignKey(Technology,on_delete=models.CASCADE)
    script_chart = models.TextField(blank=False, default='')
    script_data = models.TextField(blank=False, default='')
    type = models.TextField(blank=False, default='')
    title = models.TextField(blank=False, default='')
    is_default = models.BooleanField(default=False)
    interval = models.IntegerField(blank=False,default=60)

class MonUnitsConnections(models.Model):
    unit = models.IntegerField(blank=False,default=None)
    user = models.ForeignKey(User,on_delete=models.CASCADE)
    connection = models.ForeignKey(Connection,on_delete=models.CASCADE)
    interval = models.IntegerField(blank=False,default=60)
    plugin_name = models.TextField(blank=False, default='')


class ConfigHistory(models.Model):
    user = models.ForeignKey(User, on_delete=models.CASCADE, null=True)
    connection = models.ForeignKey(Connection,on_delete=models.CASCADE)
    start_time = models.DateTimeField()
    config_snapshot = models.TextField(blank=False)
    commit_comment = models.TextField(blank=True)
    

class Job(models.Model):
    id = models.TextField(null=False, primary_key=True)
    user = models.ForeignKey(User, on_delete=models.CASCADE, null=True)
    command = models.TextField(blank=False)
    description = models.TextField(blank=False)
    arguments =  models.TextField(blank=True)
    logdir = models.TextField()
    start_time = models.DateTimeField(null=True)
    end_time = models.DateTimeField(null=True)
    exit_code = models.IntegerField(null=True)
    utility_pid = models.IntegerField(null=True)
    process_state = models.IntegerField(null=True)
    connection = models.ForeignKey(Connection,on_delete=models.CASCADE, null=True)
