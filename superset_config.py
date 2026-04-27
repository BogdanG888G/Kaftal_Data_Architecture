# superset_config.py
import os
from flask_appbuilder.security.manager import AUTH_DB

# Основные настройки
SECRET_KEY = os.environ.get('SUPERSET_SECRET_KEY', 'supersecret123')

# Настройки языка
BABEL_DEFAULT_LOCALE = 'ru'
LANGUAGES = {
    'ru': {'flag': 'ru', 'name': 'Русский'},
    'en': {'flag': 'us', 'name': 'English'}
}

# Пользовательские переводы для основных элементов
CUSTOM_TRANSLATIONS = {
    'ru': {
        'Dashboard': 'Дашборд',
        'Charts': 'Графики',
        'Datasets': 'Датасеты',
        'SQL Lab': 'SQL Лаборатория',
        'Data': 'Данные',
        'Settings': 'Настройки',
        'Save': 'Сохранить',
        'Cancel': 'Отмена',
        'Delete': 'Удалить',
        'Edit': 'Редактировать',
        'Create': 'Создать',
        'Filter': 'Фильтр',
        'Search': 'Поиск',
        'Export': 'Экспорт',
        'Import': 'Импорт',
        'Run': 'Запустить',
        'Stop': 'Остановить',
        'Refresh': 'Обновить',
        'Download': 'Скачать',
        'Upload': 'Загрузить',
        'Add': 'Добавить',
        'Remove': 'Удалить',
        'Name': 'Название',
        'Description': 'Описание',
        'Owner': 'Владелец',
        'Created': 'Создано',
        'Modified': 'Изменено',
        'Actions': 'Действия',
        'No data': 'Нет данных',
        'Loading...': 'Загрузка...',
        'Error': 'Ошибка',
        'Success': 'Успешно',
        'Warning': 'Предупреждение',
        'Info': 'Информация',
        'Confirm': 'Подтвердить',
        'Close': 'Закрыть',
        'Back': 'Назад',
        'Next': 'Далее',
        'Finish': 'Завершить',
        'Copy': 'Копировать',
        'Paste': 'Вставить',
        'Undo': 'Отменить',
        'Redo': 'Повторить',
        'Zoom in': 'Приблизить',
        'Zoom out': 'Отдалить',
        'Full screen': 'Полный экран',
        'Share': 'Поделиться',
        'Embed': 'Встроить',
        'API': 'API',
        'Documentation': 'Документация',
        'Community': 'Сообщество',
        'Profile': 'Профиль',
        'Logout': 'Выйти',
        'Login': 'Войти',
        'Register': 'Регистрация',
        'Password': 'Пароль',
        'Username': 'Имя пользователя',
        'Email': 'Электронная почта',
    }
}

# Настройки базы данных
SQLALCHEMY_DATABASE_URI = 'sqlite:////app/superset_home/superset.db'

# Настройки отображения
SUPERSET_WEBSERVER_PORT = 8088
ENABLE_PROXY_FIX = True

# Настройки безопасности
AUTH_TYPE = AUTH_DB