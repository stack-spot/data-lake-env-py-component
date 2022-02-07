from .service import LakeFormationResource


class LakeFormation:
    """
    TO DO
    """
    @staticmethod
    def create_classifications(account_id: str, region: str) -> None:
        lakeformation = LakeFormationResource(region)
        lakeformation.create_classifications(account_id)
