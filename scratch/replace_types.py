import re

with open('main.go', 'r', encoding='utf-8') as f:
    content = f.read()

models = ['ScheduleItem', 'DeviceTimerItem', 'SceneAction', 'SceneItem', 'PowerGuardConfig', 'BudgetSettings', 'TelegramConfig']
for model in models:
    content = re.sub(r'\b' + model + r'\b', 'domain.' + model, content)

with open('main.go', 'w', encoding='utf-8') as f:
    f.write(content)
