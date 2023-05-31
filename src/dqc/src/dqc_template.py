class DataQualityPipelineTemplate:
    def __call__(self, data):
        val_report = self.validation_report(data)
        stats_report = self.stats_report(data)
        return self.create_report(val=val_report,
                                  stats=stats_report)

    def validation_report(self, data):
        """creates a report on the correctness and consistency of the data"""
        raise NotImplementedError()

    def stats_report(self, data):
        """creates a report on the statistical properties of the data"""
        raise NotImplementedError()

    def create_report(self, val, stats):
        """summarizes and implements the validation and statistical report"""
        raise NotImplementedError()
