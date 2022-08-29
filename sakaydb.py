import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

class SakayDB():
    def __init__(self, data_dir):
        self.data_dir = data_dir
        
class SakayDBError(ValueError):
    def __init__(self, exception):
        super().__init__(exception)