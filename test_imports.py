import traceback
try:
    import scripts.run.start_engine
except Exception as e:
    with open('tb.txt', 'w') as f:
        traceback.print_exc(file=f)
