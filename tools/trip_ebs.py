from collections.abc import Generator
from typing import Any

from dify_plugin import Tool
from dify_plugin.entities.tool import ToolInvokeMessage

import pandas as pd
from io import StringIO, BytesIO

class TripEbsTool(Tool):
    def _invoke(self, tool_parameters: dict[str, Any]) -> Generator[ToolInvokeMessage]:
        json_str = tool_parameters['json_str']
        try:
            df = pd.read_json(StringIO(json_str), dtype=str)
        except Exception as e:
            raise Exception(f"Error reading JSON string: {str(e)}")
          
        try:  
            mapping = {}
            
            required4 = ['部门段编码','产品分类段编码']

            required5 = ['子目段编码', 'BU段编码', 'BU往来段编码']

            required6 = ['往来段编码', '项目段编码', '备用段1编码', '备用段2编码']


            ebs_template = ['序号', '分类账', '类别', '批名', '期间', '日记帐名称', '日记账说明', '有效日期', '币种', '汇率类型',
                        '汇率日期', '汇率', '审批人', '公司段编码', '公司说明', '科目段编码', '科目段说明', '子目段编码',
                        '子目段说明','部门段编码', '部门段说明', 'BU段编码', 'BU段说明', '产品分类段编码', '产品分类段说明', '往来段编码',
                        '往来段说明', 'BU往来段编码', 'BU往来段说明', '项目段编码', '项目段说明', '备用段1编码', '备用段1说明',
                        '备用段2编码', '备用段2说明', '原币借方金额', '原币贷方金额', '行摘要', '银行流水号', '对方银行账号',
                        '付款单号', '收款流水ID', '现金流表项及往来公司', '所属BU', '本币借方金额', '本币贷方金额']

            df_ebs_template = pd.DataFrame(columns=ebs_template)
            
            for i in ebs_template:

                matches = [col for col in df.columns if col == i]

                if matches:
        
                    mapping[i] = matches[0]
                    
                else:
                    
                    if i in required4:
                        
                        df_ebs_template[i] = '0000'
                        
                    elif i in required5:
                        
                        df_ebs_template[i] = '00000'
                        
                    elif i in required6:
                        
                        df_ebs_template[i] = '000000'
                        
                        
            for i, j in mapping.items():
            
                df_ebs_template[i] = df[j]
                
            df_ebs_template['序号'] = range(1, len(df_ebs_template)+1)
            
            df_ebs_template['类别'] = '记账'
            
            df_ebs_template['原币借方金额'] = pd.to_numeric(df_ebs_template['原币借方金额'], errors='coerce').round(2)

            df_ebs_template['原币贷方金额'] = pd.to_numeric(df_ebs_template['原币贷方金额'], errors='coerce').round(2)
            
        except Exception as e:
            raise Exception(f"Error generating EBS templates: {str(e)}")
        
        # convert df to excel bytes
        excel_buffer = BytesIO()
        try:
            df_ebs_template.to_excel(excel_buffer, index=False)
            excel_buffer.seek(0)
        except Exception as e:
            raise Exception(f"Error converting DataFrame to Excel: {str(e)}")
    
        # create a blob with the excel bytes
        try:
            excel_bytes = excel_buffer.getvalue()
            filename = tool_parameters.get('filename', 'Converted Data')
            filename = f"{filename.replace(' ', '_')}.xlsx"

            yield self.create_text_message(f"Excel file '{filename}' generated successfully")

            yield self.create_blob_message(
                    blob=excel_bytes,
                    meta={
                        "mime_type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        "filename": filename
                    }
                )
        except Exception as e:
            raise Exception(f"Error creating Excel file message: {str(e)}")
