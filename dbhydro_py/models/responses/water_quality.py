# Standard library imports
from dataclasses import dataclass, field
from typing import Optional, TYPE_CHECKING, List

if TYPE_CHECKING:
    import pandas as pd

# Local imports
from dbhydro_py.models.responses.base import ApiResponseBase, Status
from dbhydro_py.utils import dataclass_from_dict

# Hierarchy of dataclasses representing the Water Quality Response structure
"""
WaterQualityResponse
│
├── values (list)
      │
      └── [0] (a single water quality record)
            │
            ├── station
            ├── project_code
            ├── date_collected
            ├── date_collected_str
            ├── sample_type
            ├── program_type
            ├── matrix
            ├── collect_method
            ├── first_trigger_date
            ├── first_trigger_date_str
            ├── depth
            ├── depth_units
            ├── test_number
            ├── parameter
            ├── data_type
            ├── value
            ├── remark_code
            ├── flag
            ├── sig_fig_value
            ├── uncertainty
            ├── dilution
            ├── mdl
            ├── pql
            ├── rdl
            ├── units
            ├── n_dec
            ├── bdl
            ├── quality_code
            ├── sample_id
            ├── up_down_stream
            ├── discharge
            ├── weather
            ├── dcs_meters
            ├── total_depth
            ├── upper_depth
            ├── lower_depth
            ├── latitude
            ├── longitude
            ├── collection_agency
            ├── work_lab
            ├── source
            ├── owner
            ├── validator
            ├── validation_level
            ├── sampling_purpose
            ├── data_investigation
            ├── receive_date
            ├── receive_date_str
            ├── measure_date
            ├── measure_date_str
            ├── method
            ├── filtration_date
            ├── filtration_date_str
            ├── sample_comment
            ├── result_comment
            ├── collection_span
            ├── lims_number
            └── storet_code
"""

@dataclass
class WaterQualityValue:
    station: str
    project_code: Optional[str] = field(metadata={"json_key": "projectCode"}, default=None)
    date_collected: Optional[int] = field(metadata={"json_key": "dateCollected"}, default=None)
    date_collected_str: Optional[str] = field(metadata={"json_key": "dateCollectedStr"}, default=None)
    sample_type: Optional[str] = field(metadata={"json_key": "sampleType"}, default=None)
    program_type: Optional[str] = field(metadata={"json_key": "programType"}, default=None)
    matrix: Optional[str] = None
    collect_method: Optional[str] = field(metadata={"json_key": "collectMethod"}, default=None)
    first_trigger_date: Optional[int] = field(metadata={"json_key": "firstTriggerDate"}, default=None)
    first_trigger_date_str: Optional[str] = field(metadata={"json_key": "firstTriggerDateStr"}, default=None)
    depth: Optional[str] = None
    depth_units: Optional[str] = field(metadata={"json_key": "depthUnits"}, default=None)
    test_number: Optional[int] = field(metadata={"json_key": "testNumber"}, default=None)
    parameter: Optional[str] = None
    data_type: Optional[str] = field(metadata={"json_key": "dataType"}, default=None)
    value: Optional[float] = None
    remark_code: Optional[str] = field(metadata={"json_key": "remarkCode"}, default=None)
    flag: Optional[str] = None
    sig_fig_value: Optional[str] = field(metadata={"json_key": "sigFigValue"}, default=None)
    uncertainty: Optional[float] = None
    dilution: Optional[float] = None
    mdl: Optional[float] = None
    pql: Optional[float] = None
    rdl: Optional[float] = None
    units: Optional[str] = None
    n_dec: Optional[int] = field(metadata={"json_key": "nDec"}, default=None)
    bdl: Optional[str] = None
    quality_code: Optional[str] = field(metadata={"json_key": "qualityCode"}, default=None)
    sample_id: Optional[str] = field(metadata={"json_key": "sampleId"}, default=None)
    up_down_stream: Optional[str] = field(metadata={"json_key": "upDownStream"}, default=None)
    discharge: Optional[str] = None
    weather: Optional[str] = None
    dcs_meters: Optional[float] = field(metadata={"json_key": "dcsMeters"}, default=None)
    total_depth: Optional[float] = field(metadata={"json_key": "totalDepth"}, default=None)
    upper_depth: Optional[float] = field(metadata={"json_key": "upperDepth"}, default=None)
    lower_depth: Optional[float] = field(metadata={"json_key": "lowerDepth"}, default=None)
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    collection_agency: Optional[str] = field(metadata={"json_key": "collectionAgency"}, default=None)
    work_lab: Optional[str] = field(metadata={"json_key": "workLab"}, default=None)
    source: Optional[str] = None
    owner: Optional[str] = None
    validator: Optional[str] = None
    validation_level: Optional[str] = field(metadata={"json_key": "validationLevel"}, default=None)
    sampling_purpose: Optional[str] = field(metadata={"json_key": "samplingPurpose"}, default=None)
    data_investigation: Optional[str] = field(metadata={"json_key": "dataInvestigation"}, default=None)
    receive_date: Optional[int] = field(metadata={"json_key": "receiveDate"}, default=None)
    receive_date_str: Optional[str] = field(metadata={"json_key": "receiveDateStr"}, default=None)
    measure_date: Optional[int] = field(metadata={"json_key": "measureDate"}, default=None)
    measure_date_str: Optional[str] = field(metadata={"json_key": "measureDateStr"}, default=None)
    method: Optional[str] = None
    filtration_date: Optional[int] = field(metadata={"json_key": "filtrationDate"}, default=None)
    filtration_date_str: Optional[str] = field(metadata={"json_key": "filtrationDateStr"}, default=None)
    sample_comment: Optional[str] = field(metadata={"json_key": "sampleComment"}, default=None)
    result_comment: Optional[str] = field(metadata={"json_key": "resultComment"}, default=None)
    collection_span: Optional[str] = field(metadata={"json_key": "collectionSpan"}, default=None)
    lims_number: Optional[str] = field(metadata={"json_key": "limsNumber"}, default=None)
    storet_code: Optional[str] = field(metadata={"json_key": "storetCode"}, default=None)


@dataclass
class WaterQualityResponse(ApiResponseBase):
    values: List[WaterQualityValue] = field(default_factory=list)

    @classmethod
    def from_dict(cls, data: dict) -> 'WaterQualityResponse':
        """
        Create a WaterQualityResponse instance from a dictionary. Passed dictionary should have a values attribute that is a list of dicts.
        
        Args:
            data (dict): The dictionary containing the water quality response data.
            
        Returns:
            WaterQualityResponse: The populated WaterQualityResponse instance.
        """
        # Ensure values is an empty list if None or missing
        if data.get('values') is None:
            data = data.copy()  # Don't modify the original
            data['values'] = []
        
        return dataclass_from_dict(cls, data)  # type: ignore

    def to_dataframe(self, include_metadata: bool = True) -> 'pd.DataFrame':
        """
        Convert water quality data to a pandas DataFrame.
        If include_metadata is False, only include essential columns.
        """
        # Attempt pandas import
        try:
            import pandas as pd
        except ImportError:
            raise ImportError('pandas is required for to_dataframe(). Install with: pip install pandas')
        
        # Include all columns
        if include_metadata:
            records = [vars(v) for v in self.values]
            df = pd.DataFrame(records)
        # Include only essential columns
        else:
            # Minimal columns for analysis/reporting
            minimal_columns = [
                'station',
                'parameter',
                'value',
                'sig_fig_value',
                'date_collected_str',
                'units'
            ]
            
            records = [
                {col: getattr(v, col, None) for col in minimal_columns} for v in self.values
            ]
            
            df = pd.DataFrame(records, columns=minimal_columns)
        
        return df
