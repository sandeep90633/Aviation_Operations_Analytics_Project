import logging

def setup_logger(name: str) -> logging.Logger:
    """
    Configures the root logging format and returns a specific logger instance
    for a module.

    Args:
        name: The name of the logger (usually the module name, e.g., __name__).

    Returns:
        The configured logging.Logger instance.
    """
    logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    handlers=[
                        logging.FileHandler(name),
                        logging.StreamHandler()
                    ])
    
    return logging