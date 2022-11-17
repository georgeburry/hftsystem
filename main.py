from backtesting import launch_backtesting_tool


if __name__ == '__main__':
    operation = input('Select an operation to run:\n\nA) Run trading system\nB) Run backtesting tool\n')
    if operation.upper() == 'A':
        raise NotImplementedError
    elif operation.upper() == 'B':
        launch_backtesting_tool()

