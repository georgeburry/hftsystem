import os
import logging
from backtesting import launch_backtesting_tool
from tasks import run_trading_tasks

logging.basicConfig(
    filename=input('Name of trading log: ') or 'trading.log',
    encoding='utf-8',
    level=int(os.getenv('LOGGING_LEVEL')) or logging.WARNING,
)


if __name__ == '__main__':
    operation = input('Select an operation to run:\n\nA) Run trading system\nB) Run backtesting tool\n\n')
    if operation.upper() == 'A':
        run_trading_tasks()
    elif operation.upper() == 'B':
        launch_backtesting_tool()

