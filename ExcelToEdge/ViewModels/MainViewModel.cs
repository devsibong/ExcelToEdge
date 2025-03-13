using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;
using CommunityToolkit.Mvvm.ComponentModel;
using CommunityToolkit.Mvvm.Input;
using ExcelToEdge.Services;
using Microsoft.Office.Interop.Excel;
using Excel = Microsoft.Office.Interop.Excel;

namespace ExcelToEdge.ViewModels
{
    public partial class MainViewModel : ObservableObject
    {
        private Excel.Application _excelApp;
        private Workbook _excelWorkbook;
        private Worksheet _excelWorksheet;
        [ObservableProperty]
        private string excelWorkbookName;
        [ObservableProperty]
        private string excelWorksheetName;
        [ObservableProperty]
        private string selectedExcelWorkbook;
        public ObservableCollection<string> CurrentExcelWorkbooks { get; } = new();

        [ObservableProperty]
        private bool isExcelConnected = false;

        public void Cleanup()
        {
            DisconnectExcel();
        }

        [RelayCommand]
        private void DisconnectExcel()
        {
            IsExcelConnected = false;
            _excelApp.SheetActivate -= ExcelApp_SheetActivate;

            if (_excelWorksheet != null)
            {
                Marshal.ReleaseComObject(_excelWorksheet);
                _excelWorksheet = null;
            }

            if (_excelWorkbook != null)
            {
                Marshal.ReleaseComObject(_excelWorkbook);
                _excelWorkbook = null;
            }

            if (_excelApp != null)
            {

                Marshal.ReleaseComObject(_excelApp.Workbooks);
                Marshal.ReleaseComObject(_excelApp);
                _excelApp = null;
            }
            CurrentExcelWorkbooks.Clear();
        }


        [RelayCommand]
        private void RefreshExcelWorkbooks()
        {
            CurrentExcelWorkbooks.Clear();
            if (_excelApp != null)
            {
                Marshal.ReleaseComObject(_excelApp.Workbooks);
                Marshal.ReleaseComObject(_excelApp);
                _excelApp = null;
            }
            Excel.Application excelApp = null;
            try
            {
                excelApp = (Excel.Application)Marshal2.GetActiveObject("Excel.Application");
                if (excelApp != null)
                {
                    _excelApp = excelApp;
                    foreach (Workbook workbook in _excelApp.Workbooks)
                    {
                        CurrentExcelWorkbooks.Add(workbook.FullName);
                    }
                    if (CurrentExcelWorkbooks.Count == 0)
                    {
                        CurrentExcelWorkbooks.Add("열려있는 Excel 워크북이 없습니다.");
                    }
                }
            }
            catch (Exception ex)
            {
                CurrentExcelWorkbooks.Add($"오류: {ex.Message}");
            }
        }

        [RelayCommand]
        private void ExcelConnected()
        {
            if (!string.IsNullOrEmpty(SelectedExcelWorkbook) && SelectedExcelWorkbook != "열려있는 Excel 워크북이 없습니다.")
            {
                if (_excelApp != null)
                {
                    foreach (Workbook workbook in _excelApp.Workbooks)
                    {
                        if (workbook.FullName == SelectedExcelWorkbook)
                        {
                            workbook.Activate();
                            IsExcelConnected = true;
                            _excelWorkbook = workbook;
                            ExcelWorkbookName = _excelWorkbook.Name;
                            _excelApp.SheetActivate -= ExcelApp_SheetActivate;
                            _excelApp.SheetActivate += ExcelApp_SheetActivate;
                            _excelWorksheet = (Worksheet)workbook.ActiveSheet;
                            ExcelWorksheetName = _excelWorksheet.Name;
                            break;
                        }
                    }
                }
            }
        }
        private void ExcelApp_SheetActivate(object sh)
        {
            if (sh is Worksheet worksheet && IsExcelConnected && _excelWorksheet != worksheet)
            {
                // 현재 활성화된 워크북 가져오기
                Workbook activeWorkbook = _excelApp.ActiveWorkbook;

                // _excelWorkbook이 null이 아니고, 활성화된 워크북이 _excelWorkbook과 동일한지 확인
                if (_excelWorkbook != null && activeWorkbook != null && activeWorkbook.FullName == _excelWorkbook.FullName)
                {
                    _excelWorksheet = worksheet;
                    ExcelWorksheetName = _excelWorksheet.Name;
                }
            }
        }

        [RelayCommand]
        private void OpenTemplate()
        {
            try
            {
                Excel.Application excelApp = null;
                try
                {
                    excelApp = (Excel.Application)Marshal2.GetActiveObject("Excel.Application");
                }
                catch (COMException)
                {
                    excelApp = new Excel.Application();
                    excelApp.Visible = true;
                    _excelApp = excelApp;
                }

                // 템플릿 파일 경로 설정
                string templatePath = System.IO.Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "Resources", "S2BTemplate.xlsx");

                // 템플릿 파일 열기
                Workbook templateWorkbook = _excelApp.Workbooks.Open(templatePath);

                // 새 워크북 생성
                _excelWorkbook = _excelApp.Workbooks.Add();

                // 템플릿의 모든 시트 복사
                foreach (Worksheet sheet in templateWorkbook.Worksheets)
                {
                    sheet.Copy(Type.Missing, _excelWorkbook.Sheets[_excelWorkbook.Sheets.Count]);
                }

                foreach (Worksheet sheet in _excelWorkbook.Worksheets)
                {
                    if (sheet.Name == "Sheet1")
                    {
                        sheet.Delete();
                        break;
                    }
                }
                // 템플릿 워크북 닫기
                templateWorkbook.Close(false);

                // 속성 업데이트
                IsExcelConnected = true;
                ExcelWorkbookName = _excelWorkbook.Name;
                _excelWorksheet = (Worksheet)_excelWorkbook.ActiveSheet;
                ExcelWorksheetName = _excelWorksheet.Name;

                // 이벤트 핸들러 재등록
                _excelApp.SheetActivate += ExcelApp_SheetActivate;
            }
            catch (Exception ex)
            {
                // 에러 처리
                Console.WriteLine($"Error opening template: {ex.Message}");
                IsExcelConnected = false;

                // ExcelApp 초기화 실패 시 정리
                if (_excelApp != null)
                {
                    _excelApp.Quit();
                    _excelApp = null;
                }
                throw;
            }
        }

    }

}
