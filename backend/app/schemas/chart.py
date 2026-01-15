from pydantic import BaseModel, Field
from typing import List, Optional, Union, Any

class ChartSeries(BaseModel):
    data: List[Union[int, float]]
    label: Optional[str] = None
    color: Optional[str] = None
    type: Optional[str] = None # 'bar', 'line', 'pie' (MUI X might separate this)

class ChartAxis(BaseModel):
    data: List[Union[str, int, float]]
    scaleType: Optional[str] = Field(None, description="'band', 'point', 'linear', 'time'")
    label: Optional[str] = None

class ChartConfig(BaseModel):
    title: str
    chartType: str = Field(..., description="'bar', 'line', 'pie', 'scatter'")
    series: List[ChartSeries]
    xAxis: List[ChartAxis] = []
    yAxis: List[ChartAxis] = []
    dataset: List[dict] = Field(default=[], description="Optional raw dataset if needed")
    
    # For Pie charts specifically
    # Pie charts in MUI X often take formatted data in series
    # We might need a flexible schema or specific subclasses
