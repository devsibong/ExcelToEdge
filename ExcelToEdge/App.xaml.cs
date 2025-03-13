using System;
using System.Configuration;
using System.Data;
using System.Windows;
using ExcelToEdge.Views;
using MaterialDesignThemes.Wpf;
using Microsoft.Extensions.DependencyInjection;
using System.Windows.Threading;
using ExcelToEdge.ViewModels;

namespace ExcelToEdge;

/// <summary>
/// Interaction logic for App.xaml
/// </summary>
public partial class App : Application
{
    public static ServiceProvider ServiceProvider { get; private set; } = null;
    protected override void OnStartup(StartupEventArgs e)
    {
        base.OnStartup(e);
        #region Theme 설정
        var paletteHelper = new PaletteHelper();
        Theme theme = paletteHelper.GetTheme();
        if (theme.ColorAdjustment is null)
        {
            theme.ColorAdjustment = new ColorAdjustment();
        }
        paletteHelper.SetTheme(theme);
        #endregion

        #region Dependency Injection 설정
        var services = new ServiceCollection();
        ConfigureService(services);
        ServiceProvider = services.BuildServiceProvider();
        #endregion

        AppDomain.CurrentDomain.UnhandledException += OnUnhandledException;
        DispatcherUnhandledException += OnDispatcherUnhandledException;
        TaskScheduler.UnobservedTaskException += OnUnobservedTaskException;

        // MainWindow 실행
        var mainWindow = ServiceProvider.GetRequiredService<MainWindow>();
        mainWindow.Show();


    }


    private void ConfigureService(IServiceCollection services)
    {
        services.AddSingleton<MainWindow>();
        services.AddSingleton<MainViewModel>();
    }

    private void OnUnhandledException(object sender, UnhandledExceptionEventArgs e)
    {
        ShowExceptionWindow(e.ExceptionObject as Exception);
    }

    private void OnDispatcherUnhandledException(object sender, DispatcherUnhandledExceptionEventArgs e)
    {
        ShowExceptionWindow(e.Exception);
        e.Handled = true;
    }

    private void OnUnobservedTaskException(object sender, UnobservedTaskExceptionEventArgs e)
    {
        ShowExceptionWindow(e.Exception);
        e.SetObserved();
    }

    private void ShowExceptionWindow(Exception ex)
    {
        if (ex == null) return;
        System.Windows.Application.Current.Dispatcher.Invoke(() =>
        {
            new ExceptionWindow(ex.Message).ShowDialog();
        });
    }
}

