def ask_confirmation(prompt: str) -> bool:
    """
    Prompt the user to answer Y/N. Keeps asking until valid input is given.

    Returns True for 'Y', False for 'N'.
    """
    while True:
        response = input(f"{prompt} (Y/N): ").strip().upper()
        if response == 'Y':
            return True
        elif response == 'N':
            return False
        else:
            print("Incorrect answer. Enter Y/N.")
            print()