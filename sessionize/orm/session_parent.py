from sessionize.sa import Session


class SessionParent:
    def __init__(self, engine):
        self.engine = engine
        self.session = Session(engine)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if exc_type:
            self.session.rollback()
            raise exc_value
        else:
            self.commit()

    def commit(self):
        self.session.commit()

    def rollback(self):
        self.session.rollback()