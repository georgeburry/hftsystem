from backtesting import launch_backtesting_tool
from tasks import run_trading_tasks


if __name__ == '__main__':
    operation = input('Select an operation to run:\n\nA) Run trading system\nB) Run backtesting tool\n')
    if operation.upper() == 'A':
        run_trading_tasks()
    elif operation.upper() == 'B':
        launch_backtesting_tool()

